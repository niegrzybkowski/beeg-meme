from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import PIL
import requests
import json
from google.cloud import storage
import io

spark = SparkSession.builder.appName('analytics').getOrCreate()

#total_schema = StructType([
#    StructField("global_id", StringType()),
#    StructField("author", StringType()),
#    StructField("created_time", StringType()),
#    StructField("desc", StringType()),
#    StructField("score", IntegerType()),
#    StructField("url", StringType()),
#    StructField("source", StringType()),
#    StructField("embeddings", StringType()),
#    StructField("cluster", StringType()),
#    StructField("text", StringType()),
#    StructField("entities", StringType()),
#    StructField("sentiments", StringType())
#])
#emptyRDD = spark.sparkContext.emptyRDD()
#total_df = spark.createDataFrame(emptyRDD,total_schema)

### 1. NIFI
nifi_schema = StructType([
    StructField("global_id", StringType()),
    StructField("author", StringType()),
    StructField("created_time", StringType()),
    StructField("desc", StringType()),
    StructField("score", IntegerType()),
    StructField("url", StringType()),
    StructField("source", StringType()),
])

nifi_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "nifi2analytics") 
  .load()
)

nifi_parsed_message = nifi_raw_kafka.select(
  from_json(col("value").cast("string"), nifi_schema).alias("message")
)

nifi_parsed_message = nifi_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.author").alias("author"),
  col("message.created_time").alias("created_time"),
  col("message.desc").alias("desc"),
  col("message.score").alias("score"),
  col("message.url").alias("url"),
  col("message.source").alias("source"),
)

### 2. ViT
vit_schema = StructType([
    StructField("global_id", StringType()),
    StructField("embeddings", StringType())
])

vit_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "vit2analytics") 
  .load()
)

vit_parsed_message = vit_raw_kafka.select(
  from_json(col("value").cast("string"), vit_schema).alias("message")
)

vit_parsed_message = vit_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.embeddings").alias("embeddings")
)

### 3. Clustering
cluster_schema = StructType([
    StructField("global_id", StringType()),
    StructField("cluster", StringType())
])

cluster_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "cluster2analytics") 
  .load()
)

cluster_parsed_message = cluster_raw_kafka.select(
  from_json(col("value").cast("string"), cluster_schema).alias("message")
)

cluster_parsed_message = cluster_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.cluster").alias("cluster")
)

### 4. OCR
ocr_schema = StructType([
    StructField("global_id", StringType()),
    StructField("text", StringType())
])

ocr_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "ocr2analytics") 
  .load()
)

ocr_parsed_message = ocr_raw_kafka.select(
  from_json(col("value").cast("string"), ocr_schema).alias("message")
)

ocr_parsed_message = ocr_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.text").alias("text")
)

### 5. NER
ner_schema = StructType([
    StructField("global_id", StringType()),
    StructField("entities", StringType())
])

ner_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "ner2analytics") 
  .load()
)

ner_parsed_message = ner_raw_kafka.select(
  from_json(col("value").cast("string"), ner_schema).alias("message")
)

ner_parsed_message = ner_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.entities").alias("entities")
)


### 6. Sentiment
sent_schema = StructType([
    StructField("global_id", StringType()),
    StructField("sentiments", StringType())
])

sent_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "sentiment2analytics") 
  .load()
)

sent_parsed_message = sent_raw_kafka.select(
  from_json(col("value").cast("string"), sent_schema).alias("message")
)

sent_parsed_message = sent_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.sentiments").alias("sentiments")
)


# join
nifi_parsed_message.join(vit_parsed_message,["global_id"]) \
                   .join(cluster_parsed_message,["global_id"]) \
                   .join(ocr_parsed_message,["global_id"]) \
                   .join(ner_parsed_message,["global_id"]) \
                   .join(sent_parsed_message,["global_id"])

nifi_parsed_message.show()

#kafka_message = nifi_parsed_message.select(
#  to_json(struct(col("global_id"),
#                 col("author"),
#                 col("created_time"),
#                 col("desc"),
#                 col("score"),
#                 col("url"),
#                 col("source"),
#                 col("embeddings"),
#                 col("cluster"),
#                 col("text"), 
#                 col("entities"),
#                 col("sentiments")
#          )).alias("value")
#)

#analytics2db = (
#    kafka_message
#    .writeStream 
#    .format("kafka") 
#    .option("kafka.bootstrap.servers", "kafka-0:9092") 
#    .option("checkpointLocation", "/tmp/cluster/checkpoint/analytics")
#    .option("topic", "analytics2db")
#    .start()
#) 