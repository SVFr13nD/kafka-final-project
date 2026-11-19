"""At-least-once delivery. request_id is stable across a crash/retry."""
import time
from .common import db, producer, send
p = producer("client")
while True:
    try:
        with db() as conn:
            row = conn.execute("SELECT request_id,document FROM outbox WHERE published_at IS NULL ORDER BY request_id FOR UPDATE SKIP LOCKED LIMIT 1").fetchone()
            if row:
                send(p,"client.events",row[1]["user_id"],row[1])
                conn.execute("UPDATE outbox SET published_at=now() WHERE request_id=%s",(row[0],))
        if not row:
            time.sleep(0.3)
    except Exception as e:
        print(f"outbox_retry {type(e).__name__}: {e}",flush=True)
        time.sleep(2)
