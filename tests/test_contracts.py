import json
from pathlib import Path
import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
ROOT=Path(__file__).resolve().parents[1]
SCHEMA=json.loads((ROOT/'schemas/product.json').read_text())
VALIDATOR=Draft202012Validator(SCHEMA,format_checker=FormatChecker())
def test_example_products_match_contract():
    rows=[json.loads(x) for x in (ROOT/'data/products.jsonl').read_text().splitlines()]
    assert len({x['product_id'] for x in rows})==len(rows)
    for row in rows:VALIDATOR.validate(row)
    assert set(json.loads((ROOT/'data/banned.json').read_text()))<={x['product_id'] for x in rows}
def test_bad_price_and_timestamp_rejected():
    row=json.loads((ROOT/'data/products.jsonl').read_text().splitlines()[0])
    row['price']['amount']=-1
    with pytest.raises(ValidationError):VALIDATOR.validate(row)
    row['price']['amount']=1;row['created_at']='tomorrow'
    with pytest.raises(ValidationError):VALIDATOR.validate(row)
def test_security_and_monitoring_cover_both_clusters():
    compose=yaml.safe_load((ROOT/'compose.yaml').read_text())
    targets=yaml.safe_load((ROOT/'infra/monitoring/prometheus.yml').read_text())['scrape_configs'][0]['static_configs']
    targets={t for group in targets for t in group['targets']}
    for cluster in ['primary','secondary']:
        for i in range(1,4):
            name=f'{cluster}-{i}'
            service=compose['services'][name];env=service['environment']
            assert env['KAFKA_SSL_CLIENT_AUTH']=='required'
            assert env['KAFKA_MIN_INSYNC_REPLICAS']==2
            assert env['KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR']==3
            assert 'PLAINTEXT' not in env['KAFKA_LISTENER_SECURITY_PROTOCOL_MAP']
            assert f'{name}:7071' in targets
            assert 'ports' not in service
