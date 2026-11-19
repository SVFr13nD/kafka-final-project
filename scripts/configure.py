# Generate lab PKI and Java service properties.
import pathlib, subprocess, shutil, os
ROOT = pathlib.Path('.runtime')
ROLES = ['admin','shop','moderator','filter','client','catalog','rec-reader','lake','analytics','mirror','connect']
BROKERS = [f'{c}-{i}' for c in ['primary','secondary'] for i in range(1,4)]
def run(*args): subprocess.run(args,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
def write(path,text):
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(text)
def java_ssl(role,prefix=''):
    settings={'security.protocol':'SSL','ssl.truststore.location':'/certs/truststore.p12',
      'ssl.truststore.password':'changeit','ssl.truststore.type':'PKCS12',
      'ssl.keystore.location':f'/certs/{role}.p12','ssl.keystore.password':'changeit',
      'ssl.key.password':'changeit','ssl.keystore.type':'PKCS12','ssl.endpoint.identification.algorithm':'https'}
    return ''.join(f'{prefix}{k}={v}\n' for k,v in settings.items())
def main():
    ROOT.mkdir(exist_ok=True)
    ca=ROOT/'ca'; ca.mkdir(exist_ok=True)
    if not (ca/'ca.crt').exists():
        run('openssl','req','-x509','-newkey','rsa:3072','-nodes','-days','365','-subj','/CN=marketplace-lab-ca','-keyout',str(ca/'ca.key'),'-out',str(ca/'ca.crt'))
    for role in BROKERS+ROLES:
        d=ROOT/'roles'/role; d.mkdir(parents=True,exist_ok=True)
        if not (d/f'{role}.p12').exists():
            run('openssl','req','-new','-newkey','rsa:2048','-nodes','-subj',f'/CN={role}','-keyout',str(d/f'{role}.key'),'-out',str(d/'request.csr'))
            write(d/'extensions',f'subjectAltName=DNS:{role}\nextendedKeyUsage=serverAuth,clientAuth\n')
            run('openssl','x509','-req','-in',str(d/'request.csr'),'-CA',str(ca/'ca.crt'),'-CAkey',str(ca/'ca.key'),'-CAcreateserial','-days','365','-extfile',str(d/'extensions'),'-out',str(d/f'{role}.crt'))
            run('openssl','pkcs12','-export','-in',str(d/f'{role}.crt'),'-inkey',str(d/f'{role}.key'),'-certfile',str(ca/'ca.crt'),'-name',role,'-passout','pass:changeit','-out',str(d/f'{role}.p12'))
            shutil.copy(ca/'ca.crt',d/'ca.crt')
            run('keytool','-importcert','-noprompt','-alias','ca','-file',str(d/'ca.crt'),'-keystore',str(d/'truststore.p12'),'-storetype','PKCS12','-storepass','changeit')
            (d/'request.csr').unlink(); (d/'extensions').unlink()
        write(d/'client.properties',java_ssl(role))
    # Root-only CA key; runtime containers receive only their own identity.
    os.chmod(ca/'ca.key',0o600)
    primary=','.join(f'primary-{i}:9092' for i in range(1,4))
    secondary=','.join(f'secondary-{i}:9092' for i in range(1,4))
    write(ROOT/'connect.properties',f'''bootstrap.servers={primary}
rest.port=8083
rest.advertised.host.name=connect
group.id=file-connect
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
plugin.path=/opt/kafka/libs/connect-file-3.9.1.jar
consumer.isolation.level=read_committed
''' + java_ssl('connect') + java_ssl('connect','consumer.') + java_ssl('connect','producer.'))
    write(ROOT/'mirror.properties',f'''clusters=primary,secondary
primary.bootstrap.servers={primary}
secondary.bootstrap.servers={secondary}
primary->secondary.enabled=true
primary->secondary.topics=products.filtered,client.events
primary->secondary.groups=.*
primary->secondary.emit.checkpoints.enabled=false
primary->secondary.emit.heartbeats.enabled=true
primary->secondary.sync.topic.acls.enabled=false
primary->secondary.sync.topic.configs.enabled=true
primary->secondary.refresh.topics.interval.seconds=10
replication.factor=3
checkpoints.topic.replication.factor=3
heartbeats.topic.replication.factor=3
offset-syncs.topic.replication.factor=3
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
primary.consumer.isolation.level=read_committed
''' + java_ssl('mirror','primary.') + java_ssl('mirror','secondary.'))
    print('PKI and configuration ready (existing certificates preserved)')
if __name__=='__main__': main()
