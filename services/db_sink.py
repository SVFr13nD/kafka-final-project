import json
import sys
from psycopg.types.json import Jsonb
from .common import consumer, db
kind = sys.argv[1]
if kind not in ("products", "recommendations"):
    raise ValueError(kind)
c = consumer("catalog" if kind == "products" else "rec-reader", f"db-{kind}",
             ["products.filtered" if kind == "products" else "recommendations"],
             "primary" if kind == "products" else "secondary")
try:
    while True:
        msg = c.poll(1)
        if msg is None: continue
        if msg.error(): raise RuntimeError(msg.error())
        key = msg.key().decode()
        value = json.loads(msg.value()) if msg.value() is not None else None
        with db() as conn:
            if kind == "products":
                changed = conn.execute("""INSERT INTO product_offsets VALUES (%s,%s,%s)
                    ON CONFLICT(product_id) DO UPDATE SET kafka_offset=EXCLUDED.kafka_offset
                    WHERE product_offsets.kafka_partition=EXCLUDED.kafka_partition
                    AND product_offsets.kafka_offset<EXCLUDED.kafka_offset RETURNING product_id""",
                    (key,msg.partition(),msg.offset())).fetchone()
                if changed:
                    if value is None:
                        conn.execute("DELETE FROM products WHERE product_id=%s",(key,))
                    else:
                        conn.execute("""INSERT INTO products VALUES (%s,%s,%s,%s) ON CONFLICT(product_id)
                          DO UPDATE SET document=EXCLUDED.document,kafka_partition=EXCLUDED.kafka_partition,kafka_offset=EXCLUDED.kafka_offset""",
                          (key,Jsonb(value),msg.partition(),msg.offset()))
            elif value is not None:
                conn.execute("""INSERT INTO recommendations VALUES (%s,%s,%s,%s) ON CONFLICT(user_id)
                  DO UPDATE SET document=EXCLUDED.document,kafka_offset=EXCLUDED.kafka_offset
                  WHERE recommendations.kafka_partition=EXCLUDED.kafka_partition
                  AND recommendations.kafka_offset<EXCLUDED.kafka_offset""",(key,Jsonb(value),msg.partition(),msg.offset()))
        c.commit(message=msg,asynchronous=False)
        print(f"db_saved {kind} {key} {msg.partition()}:{msg.offset()}",flush=True)
finally:
    c.close()
