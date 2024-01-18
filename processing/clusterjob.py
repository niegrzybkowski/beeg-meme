# Run with:
# spark-submit --name clusterjob --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 clusterjob.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pickle
from sklearn.cluster import Birch

spark = SparkSession.builder.appName('vit').getOrCreate()

model = Birch()
n_items = [0]

# scikit complains about clusters
import warnings
warnings.filterwarnings("error")

@udf(IntegerType())
def process(global_id, embeddings):
    print("[UDF] Processing ", global_id)
    if embeddings == "":
        return -1
    try:
        embeddings = json.loads(embeddings)
        model.partial_fit([embeddings])
        value = int(model.predict([embeddings])[0])
        n_items[0] = n_items[0] + 1
        if n_items[0] % 10 == 0:
            with open(f"/models/birch_{n_items}", "w") as f:
                pickle.dump(model, f)
        return value
    except Exception:
        import traceback
        print(traceback.format_exc())
        return -2

schema = StructType([
    StructField("global_id", StringType()),
    StructField("embeddings", StringType())
])

raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "vit2cluster") 
  .load()
)

parsed_message = raw_kafka.select(
  from_json(col("value").cast("string"), schema).alias("message")
)
parsed_message = parsed_message.withColumn("timestamp",current_timestamp())

embedded = parsed_message.select(
  col("message.global_id").alias("global_id"),
  process(col("message.global_id"), col("message.embeddings")).alias("cluster"),
  col("timestamp")
)

kafka_message = embedded.select(
  to_json(struct(col("global_id"), col("cluster"), col("timestamp"))).alias("value")
)

cluster2analytics_ssc = (
    kafka_message
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "kafka-0:9092") 
    .option("checkpointLocation", "/tmp/cluster/checkpoint/analytics")
    .option("topic", "cluster2analytics")
    .start()
)

cluster2analytics_ssc.awaitTermination()
