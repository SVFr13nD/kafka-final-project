"""SHOP API, CLIENT API and ban list emulation from the terminal."""
import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.types.json import Jsonb
from .common import producer, send, db

SCHEMA = json.loads(Path("schemas/product.json").read_text())

def request(user, kind, extra):
    event = {"request_id": str(uuid.uuid4()), "user_id": user, "event_type": kind,
             "timestamp": datetime.now(timezone.utc).isoformat(), **extra}
    # One database transaction stores the request and its future Kafka publication.
    with db() as conn:
        conn.execute("INSERT INTO client_requests(request_id,user_id,event_type,document) VALUES (%s,%s,%s,%s)",
                     (event["request_id"], user, kind, Jsonb(event)))
        conn.execute("INSERT INTO outbox(request_id,document) VALUES (%s,%s)", (event["request_id"], Jsonb(event)))
    return event

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("shop"); s.add_argument("--file", default="data/products.jsonl")
    b = sub.add_parser("ban"); b.add_argument("action", choices=["add", "remove"]); b.add_argument("product_id")
    sub.add_parser("bans")
    s = sub.add_parser("search"); s.add_argument("name"); s.add_argument("--user", required=True)
    s = sub.add_parser("recommend"); s.add_argument("--user", required=True)
    s = sub.add_parser("event"); s.add_argument("kind", choices=["view", "cart", "purchase", "review"])
    s.add_argument("product_id"); s.add_argument("--user", required=True)
    args = parser.parse_args()
    if args.command == "shop":
        rows = [json.loads(line) for line in Path(args.file).read_text().splitlines() if line.strip()]
        validator = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
        for row in rows:
            validator.validate(row)
        p = producer("shop")
        for row in rows:
            send(p, "products.raw", row["product_id"], row)
    elif args.command == "ban":
        send(producer("moderator"), "products.banned", args.product_id,
             {"blocked": args.action == "add"})
    elif args.command == "bans":
        import requests
        r = requests.get("http://filter:8090/bans", timeout=10); r.raise_for_status(); print(r.text)
    elif args.command == "search":
        event = request(args.user, "search", {"query": args.name})
        # strpos treats percent and underscore as literal characters, not SQL wildcards.
        with db() as conn:
            rows = conn.execute("SELECT document FROM products WHERE strpos(lower(document->>'name'),lower(%s))>0 ORDER BY product_id LIMIT 20", (args.name,)).fetchall()
        print(json.dumps({"request_id":event["request_id"], "products":[r[0] for r in rows]}, ensure_ascii=False))
    elif args.command == "recommend":
        event = request(args.user, "recommend", {})
        with db() as conn:
            row = conn.execute("SELECT document FROM recommendations WHERE user_id=%s", (args.user,)).fetchone()
            rec = row[0] if row else {"product_ids":[], "reason":"Run analytics after sending user events"}
            # Never serve products removed from the current approved catalog.
            ids = rec.get("product_ids", [])
            current = {r[0]:r[1] for r in conn.execute("SELECT product_id,document FROM products WHERE product_id=ANY(%s)",(ids,)).fetchall()}
        print(json.dumps({"request_id":event["request_id"], "user_id":args.user,
                          "products":[current[i] for i in ids if i in current]}, ensure_ascii=False))
    else:
        print(json.dumps(request(args.user,args.kind,{"product_id":args.product_id}),ensure_ascii=False))

if __name__ == "__main__":
    main()
