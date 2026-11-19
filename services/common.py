import json
import os
from pathlib import Path
from confluent_kafka import Producer, Consumer
import psycopg

PRIMARY = "primary-1:9092,primary-2:9092,primary-3:9092"
SECONDARY = "secondary-1:9092,secondary-2:9092,secondary-3:9092"

def config(role, cluster="primary"):
    return {"bootstrap.servers": PRIMARY if cluster == "primary" else SECONDARY,
            "security.protocol": "SSL", "ssl.ca.location": "/certs/ca.crt",
            "ssl.certificate.location": f"/certs/{role}.crt",
            "ssl.key.location": f"/certs/{role}.key",
            "ssl.endpoint.identification.algorithm": "https"}

def producer(role, cluster="primary"):
    return Producer({**config(role, cluster), "acks": "all", "enable.idempotence": True,
                     "delivery.timeout.ms": 30000})

def send(p, topic, key, value):
    errors = []
    def delivered(error, msg):
        if error:
            errors.append(str(error))
        else:
            print(json.dumps({"event": "delivered", "topic": topic, "key": key,
                              "partition": msg.partition(), "offset": msg.offset()}, ensure_ascii=False), flush=True)
    p.produce(topic, key=key, value=None if value is None else json.dumps(value, ensure_ascii=False), on_delivery=delivered)
    if p.flush(35) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")

def consumer(role, group, topics, cluster="primary"):
    c = Consumer({**config(role, cluster), "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "enable.auto.offset.store": False,
                  "isolation.level": "read_committed"})
    c.subscribe(topics)
    return c

def db():
    return psycopg.connect(os.environ["DATABASE_URL"])
