"""Executable integration assertions. A failure exits nonzero; evidence is real output."""
import json,time,argparse,uuid,ssl,socket
from pathlib import Path
import requests
from confluent_kafka import Producer, KafkaError, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from .common import config,db

def wait(description,fn,seconds=180):
    deadline=time.monotonic()+seconds; last=None
    while time.monotonic()<deadline:
        try:
            value=fn()
            if value: print('PASS',description,flush=True); return value
        except Exception as exc: last=str(exc)
        time.sleep(2)
    raise RuntimeError(f'Timeout: {description}; last error: {last}')

def http_json(url):
    r=requests.get(url,timeout=5);r.raise_for_status();return r.json()
def sql(statement,params=()):
    with db() as c:return c.execute(statement,params).fetchall()
def snapshot(topic,cluster='secondary'):
    # Read exactly the captured log range, ignoring transaction control records.
    c=Consumer({**config('admin',cluster),'group.id':'verify-'+str(uuid.uuid4()),'enable.auto.commit':False,'isolation.level':'read_committed'})
    result={}
    try:
        md=c.list_topics(topic,timeout=10).topics[topic]
        for partition in md.partitions:
            tp=TopicPartition(topic,partition)
            low,high=c.get_watermark_offsets(tp,timeout=10,cached=False)
            c.assign([TopicPartition(topic,partition,low)])
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                m=c.poll(.5)
                if m and not m.error():
                    result[m.key().decode() if m.key() else str(m.offset())]=None if m.value() is None else json.loads(m.value())
                pos=c.position([tp])[0].offset
                if pos>=high:break
    finally:c.close()
    return result

def catalog_ready():
    rows=sql('SELECT product_id FROM products ORDER BY product_id')
    return [x[0] for x in rows]==['p001','p002','p003','p004','p005']
def hdfs_files(path):
    r=requests.get('http://namenode:9870/webhdfs/v1'+path,params={'op':'LISTSTATUS','user.name':'root'},timeout=10)
    r.raise_for_status(); result=[]
    for x in r.json()['FileStatuses']['FileStatus']:
        p=path+'/'+x['pathSuffix']
        if x['type']=='DIRECTORY': result+=hdfs_files(p)
        else: result.append(p)
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('stage',choices=['ready','banned','data','recommendations','absent','present','metrics','alerts','topology'])
    p.add_argument('--product',default='p001');p.add_argument('--status',default='firing');args=p.parse_args()
    if args.stage=='ready':
        wait('PostgreSQL',lambda:sql('SELECT 1'))
        wait('Kafka Streams RUNNING',lambda:requests.get('http://filter:8090/health',timeout=5).status_code==200)
        wait('HDFS datanode registered',lambda:len(http_json('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState')['beans']) and http_json('http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState')['beans'][0]['NumLiveDataNodes']>=1)
        wait('Kafka Connect',lambda:http_json('http://connect:8083/connector-plugins'))
        body=json.loads(Path('infra/connect-sink.json').read_text()) if Path('infra/connect-sink.json').exists() else {'config':{'connector.class':'org.apache.kafka.connect.file.FileStreamSinkConnector','tasks.max':'1','topics':'products.filtered','file':'/debug/products.jsonl'}}
        r=requests.put('http://connect:8083/connectors/approved-products-file/config',json=body['config'],timeout=10);r.raise_for_status()
        wait('File connector running',lambda:all(t['state']=='RUNNING' for t in http_json('http://connect:8083/connectors/approved-products-file/status')['tasks']) and len(http_json('http://connect:8083/connectors/approved-products-file/status')['tasks'])>0)
    elif args.stage=='banned':
        wait('Ban applied',lambda:http_json('http://filter:8090/bans').get(args.product) is True)
    elif args.stage=='data':
        wait('Five approved products in PostgreSQL',catalog_ready)
        wait('Client outbox drained',lambda:sql('SELECT count(*) FROM outbox WHERE published_at IS NULL')[0][0]==0)
        mirrored=wait('MirrorMaker2 copied approved catalog',lambda:(r if all(r.get(k) for k in ['p001','p002','p003','p004','p005']) and not r.get('p006') else None) if (r:=snapshot('primary.products.filtered')) else None)
        wait('HDFS products and client events',lambda:len(hdfs_files('/marketplace/products'))>=5 and len(hdfs_files('/marketplace/events'))>=6)
        def check_file():
            path=Path('/debug/products.jsonl')
            if not path.exists():return False
            rows=[json.loads(x) for x in path.read_text().splitlines() if x.strip() and x.strip()!='null']
            ids={x['product_id'] for x in rows if x}
            return {'p001','p002','p003','p004','p005'}<=ids and 'p006' not in ids
        wait('Kafka Connect wrote only approved products',check_file)
        Path('/evidence/mirrored-products.json').write_text(json.dumps(mirrored,ensure_ascii=False,indent=2))
    elif args.stage=='recommendations':
        def recs():
            rows=dict(sql('SELECT user_id,document FROM recommendations'))
            return rows if rows.get('alice',{}).get('product_ids',[])[:2]==['p001','p002'] and rows.get('bob',{}).get('product_ids',[])[:2]==['p003','p004'] else None
        rows=wait('Personalized recommendations for Alice and Bob',recs)
        Path('/evidence/recommendations.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2))
        wait('Recommendations in secondary Kafka',lambda:len(snapshot('recommendations'))>=2)
    elif args.stage in ['absent','present']:
        wait(f'{args.product} {args.stage} in catalog',lambda:bool(sql('SELECT 1 FROM products WHERE product_id=%s',(args.product,)))==(args.stage=='present'))
    elif args.stage=='metrics':
        def metrics():
            result=http_json('http://prometheus:9090/api/v1/query?query=up%7Bjob%3D%22kafka%22%7D')['data']['result']
            return len(result)==6 and all(x['value'][1]=='1' for x in result)
        wait('All six JMX targets up',metrics)
        wait('JMX Kafka metrics present',lambda:len(http_json('http://prometheus:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions')['data']['result'])==6)
        r=requests.get('http://grafana:3000/api/dashboards/uid/marketplace-kafka',auth=('admin','marketplace-demo'),timeout=10);r.raise_for_status()
        assert len(r.json()['dashboard']['panels'])>=6
        print('PASS Grafana dashboard provisioned')
    elif args.stage=='alerts':
        def alerts():
            path=Path('/evidence/alerts.jsonl')
            if not path.exists():return False
            return any(a.get('status')==args.status and a['labels'].get('alertname')=='KafkaBrokerDown' and a['labels'].get('instance')=='primary-3:7071' for line in path.read_text().splitlines() for a in json.loads(line).get('payload',{}).get('alerts',[]))
        wait('Alertmanager delivered '+args.status,alerts)
    elif args.stage=='topology':
        evidence={}
        for cluster in ['primary','secondary']:
            a=AdminClient(config('admin',cluster)); md=a.list_topics(timeout=15)
            assert len(md.brokers)==3
            topic='products.raw' if cluster=='primary' else 'primary.products.filtered'
            t=md.topics[topic]; assert len(t.partitions)==3
            assert all(len(x.replicas)==3 and len(x.isrs)>=2 for x in t.partitions.values())
            resource=ConfigResource(ConfigResource.Type.TOPIC,topic)
            cfg=a.describe_configs([resource])[resource].result()
            assert cfg['min.insync.replicas'].value=='2'
            evidence[cluster]={'topic':topic,'min.insync.replicas':2,'partitions':[{ 'id':i,'replicas':v.replicas,'isr':v.isrs,'leader':v.leader} for i,v in t.partitions.items()]}
        Path('/evidence/topology.json').write_text(json.dumps(evidence,indent=2));print('PASS RF=3, partitions=3, min ISR=2 on both clusters')
if __name__=='__main__': main()
