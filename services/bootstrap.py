import time
from confluent_kafka.admin import AdminClient, NewTopic, AclBinding, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
from .common import config

def acl(role,kind,name,operations,prefix=False):
    return [AclBinding(getattr(ResourceType,kind),name,
        ResourcePatternType.PREFIXED if prefix else ResourcePatternType.LITERAL,
        'User:'+role,'*',getattr(AclOperation,op),AclPermissionType.ALLOW) for op in operations]

def main():
    for cluster in ['primary','secondary']:
        admin=AdminClient(config('admin',cluster))
        for attempt in range(90):
            try:
                metadata=admin.list_topics(timeout=5)
                if len(metadata.brokers)==3: break
            except Exception: pass
            time.sleep(2)
        else: raise RuntimeError(f'{cluster}: expected 3 brokers')
        topics = {'products.raw':'delete','products.banned':'compact','products.filtered':'compact','products.invalid':'delete','client.events':'delete',
                  'connect-configs':'compact','connect-offsets':'compact','connect-status':'compact'} if cluster=='primary' else {
                  'primary.products.filtered':'compact','primary.client.events':'delete','recommendations':'compact'}
        existing=admin.list_topics(timeout=10).topics
        new=[NewTopic(t,num_partitions=1 if t=='connect-configs' else 3,replication_factor=3,
             config={'cleanup.policy':policy,'min.insync.replicas':'2','retention.ms':'86400000','segment.bytes':'104857600','delete.retention.ms':'86400000'}) for t,policy in topics.items() if t not in existing]
        for topic,future in admin.create_topics(new).items(): future.result(); print('Created',cluster,topic,flush=True)
        bindings=[]
        def grant(*args,**kw): bindings.extend(acl(*args,**kw))
        if cluster=='primary':
            for role,topic in [('shop','products.raw'),('moderator','products.banned'),('client','client.events')]:
                grant(role,'TOPIC',topic,['WRITE','DESCRIBE'])
                grant(role,'CLUSTER','kafka-cluster',['IDEMPOTENT_WRITE'])
            grant('filter','TOPIC','products.raw',['READ','DESCRIBE'])
            grant('filter','TOPIC','products.banned',['READ','DESCRIBE'])
            for t in ['products.filtered','products.invalid']: grant('filter','TOPIC',t,['WRITE','DESCRIBE'])
            grant('filter','TOPIC','product-filter',['ALL'],prefix=True)
            grant('filter','GROUP','product-filter',['READ','DESCRIBE'],prefix=True)
            grant('filter','TRANSACTIONAL_ID','product-filter',['WRITE','DESCRIBE'],prefix=True)
            grant('filter','CLUSTER','kafka-cluster',['IDEMPOTENT_WRITE'])
            grant('catalog','TOPIC','products.filtered',['READ','DESCRIBE'])
            grant('catalog','GROUP','db-products',['READ','DESCRIBE'])
            grant('connect','TOPIC','products.filtered',['READ','DESCRIBE'])
            grant('connect','TOPIC','connect-',['ALL'],prefix=True)
            grant('connect','GROUP','connect-',['READ','DESCRIBE'],prefix=True)
            grant('connect','GROUP','file-connect',['READ','DESCRIBE'])
            grant('connect','CLUSTER','kafka-cluster',['IDEMPOTENT_WRITE'])
            for t in ['products.filtered','client.events']: grant('mirror','TOPIC',t,['READ','DESCRIBE','DESCRIBE_CONFIGS'])
        else:
            for t in ['primary.products.filtered','primary.client.events']: grant('lake','TOPIC',t,['READ','DESCRIBE'])
            grant('lake','GROUP','lake-sink',['READ','DESCRIBE'])
            grant('analytics','TOPIC','recommendations',['WRITE','DESCRIBE'])
            grant('analytics','CLUSTER','kafka-cluster',['IDEMPOTENT_WRITE'])
            grant('rec-reader','TOPIC','recommendations',['READ','DESCRIBE'])
            grant('rec-reader','GROUP','db-recommendations',['READ','DESCRIBE'])
            grant('mirror','TOPIC','primary.',['ALL'],prefix=True)
            grant('mirror','TOPIC','heartbeats',['ALL'])
        # MM2 writes its own internal topics on both clusters (source offset-syncs).
        for prefix in ['mm2-','mirrormaker2-']:
            grant('mirror','TOPIC',prefix,['ALL'],prefix=True)
        grant('mirror','GROUP','mirrormaker2-',['ALL'],prefix=True)
        grant('mirror','GROUP','mm2-',['ALL'],prefix=True)
        grant('mirror','CLUSTER','kafka-cluster',['IDEMPOTENT_WRITE','DESCRIBE'])
        for future in admin.create_acls(bindings): future.result()
        print('ACL ready:',cluster,flush=True)
if __name__=='__main__': main()
