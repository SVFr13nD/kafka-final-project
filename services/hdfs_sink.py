"""Persist secondary-cluster records before acknowledging their offsets."""
import json
import requests
from .common import consumer
base="http://namenode:9870/webhdfs/v1"
c=consumer("lake","lake-sink",["primary.products.filtered","primary.client.events"],"secondary")
try:
    while True:
        msg=c.poll(1)
        if msg is None: continue
        if msg.error(): raise RuntimeError(msg.error())
        value=json.loads(msg.value()) if msg.value() is not None else {"_deleted":True}
        value.update({"_key":msg.key().decode(),"_partition":msg.partition(),"_offset":msg.offset()})
        dataset="products" if msg.topic().endswith("products.filtered") else "events"
        directory=f"/marketplace/{dataset}/{msg.partition()}"
        r=requests.put(base+directory,params={"op":"MKDIRS","user.name":"root"},timeout=15)
        r.raise_for_status()
        if not r.json().get("boolean"): raise RuntimeError("HDFS mkdir failed")
        path=f"{directory}/{msg.offset()}.json"
        r=requests.put(base+path,params={"op":"CREATE","overwrite":"true","user.name":"root"},allow_redirects=False,timeout=15)
        if r.status_code!=307: raise RuntimeError(f"HDFS create: {r.status_code} {r.text}")
        r=requests.put(r.headers["Location"],data=(json.dumps(value,ensure_ascii=False)+"\n").encode(),timeout=30)
        r.raise_for_status()
        c.commit(message=msg,asynchronous=False)
        print(f"hdfs_saved {path}",flush=True)
finally:
    c.close()
