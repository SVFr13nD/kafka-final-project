"""Small batch recommendation job over HDFS snapshots; no ML model required."""
import json
from datetime import datetime,timezone
from pyspark.sql import SparkSession, Window, functions as F
from .ranking import weights
from services.common import producer,send

def main():
    spark=(SparkSession.builder.master('local[2]').appName('marketplace-recommendations')
        .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','2')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    root='hdfs://namenode:9000/marketplace'
    products=spark.read.option('recursiveFileLookup','true').json(root+'/products')
    if '_deleted' not in products.columns: products=products.withColumn('_deleted',F.lit(False))
    latest=Window.partitionBy('_key').orderBy(F.col('_offset').desc())
    products=(products.withColumn('rn',F.row_number().over(latest)).filter('rn=1')
        .filter(~F.coalesce(F.col('_deleted'),F.lit(False)))
        .select('product_id','category','name','price'))
    events=spark.read.option('recursiveFileLookup','true').json(root+'/events').dropDuplicates(['request_id'])
    users=events.select('user_id').distinct()
    weighted=events.withColumn('weight',F.create_map(*[F.lit(x) for pair in weights.items() for x in pair])[F.col('event_type')])
    interest=(weighted.join(products.select('product_id','category'),'product_id','inner')
        .groupBy('user_id','category').agg(F.sum('weight').alias('score')))
    ranked=(users.crossJoin(products).join(interest,['user_id','category'],'left').fillna({'score':0})
        .withColumn('rank',F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('score'),F.asc('product_id'))))
        .filter('rank<=3'))
    # Driver collects only three items per user. This is intentionally a small lab batch.
    rows=ranked.orderBy('user_id','rank').collect()
    result={}
    for row in rows:
        result.setdefault(row.user_id,[]).append(row.product_id)
    generated_at=datetime.now(timezone.utc).isoformat()
    payloads=[{'user_id':u,'product_ids':ids,'generated_at':generated_at,'algorithm':'category-interest-v1'} for u,ids in result.items()]
    if not payloads: raise RuntimeError('No recommendations: send products and events first')
    spark.createDataFrame([(json.dumps(p,ensure_ascii=False),) for p in payloads],['value']).write.mode('overwrite').text(root+'/recommendations/latest')
    p=producer('analytics','secondary')
    for row in payloads: send(p,'recommendations',row['user_id'],row)
    print(json.dumps({'users':len(payloads),'recommendations':payloads},ensure_ascii=False),flush=True)
    spark.stop()
if __name__=='__main__': main()
