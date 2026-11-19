"""Run with SHOP identity: writing raw is allowed, filtered/events are forbidden."""
from confluent_kafka import Producer,KafkaError
from .common import config,send
import json
from pathlib import Path
p=Producer({**config('shop'),'acks':'all','enable.idempotence':True,'delivery.timeout.ms':10000})
row=json.loads(Path('data/products.jsonl').read_text().splitlines()[0])
send(p,'products.raw',row['product_id'],row)
for topic in ['products.filtered','client.events','products.banned']:
    errors=[]
    p.produce(topic,key='forbidden',value='{}',on_delivery=lambda error,msg:errors.append(error))
    assert p.flush(15)==0
    assert errors and errors[0] is not None and errors[0].code()==KafkaError.TOPIC_AUTHORIZATION_FAILED,errors
    print('PASS shop denied:',topic,flush=True)
# TLS connection without client certificate must not obtain Kafka metadata.
from confluent_kafka.admin import AdminClient
cfg=config('shop');del cfg['ssl.certificate.location'];del cfg['ssl.key.location']
try: AdminClient(cfg).list_topics(timeout=5)
except Exception: print('PASS client certificate required',flush=True)
else: raise AssertionError('Unauthenticated metadata succeeded')
