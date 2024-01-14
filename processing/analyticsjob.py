from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import PIL
import requests
import json
#from google.cloud import storage
import io
import time

spark = SparkSession.builder.appName('analytics').getOrCreate()

#
# Schemas
#
nifi_schema = StructType([
    StructField("global_id", StringType()),
    StructField("author", StringType()),
    StructField("created_time", StringType()),
    StructField("desc", StringType()),
    StructField("score", IntegerType()),
    StructField("url", StringType()),
    StructField("source", StringType()),
])
vit_schema = StructType([
    StructField("global_id", StringType()),
    StructField("embeddings", StringType())
])
cluster_schema = StructType([
    StructField("global_id", StringType()),
    StructField("cluster", StringType())
])
ocr_schema = StructType([
    StructField("global_id", StringType()),
    StructField("text", StringType())
])
ner_schema = StructType([
    StructField("global_id", StringType()),
    StructField("entities", StringType())
])
sent_schema = StructType([
    StructField("global_id", StringType()),
    StructField("sentiments", StringType())
])

#
# Read streams
#
nifi_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "nifi2analytics") 
  .load()
)
vit_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "vit2analytics")
  .load()
)
cluster_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "cluster2analytics") 
  .load()
)
ocr_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "ocr2analytics") 
  .load()
)
ner_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "ner2analytics") 
  .load()
)
sent_raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "sentiment2analytics") 
  .load()
)

#
# Parse messages
#
### 1. NIFI
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
vit_parsed_message = vit_raw_kafka.select(
  from_json(col("value").cast("string"), vit_schema).alias("message")
)
vit_parsed_message = vit_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.embeddings").alias("embeddings")
)

### 3. Clustering
cluster_parsed_message = cluster_raw_kafka.select(
  from_json(col("value").cast("string"), cluster_schema).alias("message")
)
cluster_parsed_message = cluster_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.cluster").alias("cluster")
)

### 4. OCR
ocr_parsed_message = ocr_raw_kafka.select(
  from_json(col("value").cast("string"), ocr_schema).alias("message")
)
ocr_parsed_message = ocr_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.text").alias("text")
)

### 5. NER
ner_parsed_message = ner_raw_kafka.select(
  from_json(col("value").cast("string"), ner_schema).alias("message")
)
ner_parsed_message = ner_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.entities").alias("entities")
)

### 6. Sentiment
sent_parsed_message = sent_raw_kafka.select(
  from_json(col("value").cast("string"), sent_schema).alias("message")
)
sent_parsed_message = sent_parsed_message.select(
  col("message.global_id").alias("global_id"),
  col("message.sentiments").alias("sentiments")
)

#
# Join streams
#
#joined_df = \
nifi_parsed_message.join(vit_parsed_message,"global_id") \
                   .join(cluster_parsed_message,"global_id") \
                   .join(ocr_parsed_message,"global_id") \
                   .join(ner_parsed_message,"global_id") \
                   .join(sent_parsed_message,"global_id")

#
# Output the resulting dataframe
#
query = nifi_parsed_message.outputMode("append").writeStream.format("console").start()
#query = joined_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()


#kafka_message = joined_df.select(
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
#analytics2test_ssc = (
#    kafka_message
#    .writeStream 
#    .format("kafka") 
#    .option("kafka.bootstrap.servers", "kafka-0:9092") 
#    .option("checkpointLocation","c:/kafka/kafka-logs")
#    .option("topic", "test")
#    .start()
#)
#analytics2test_ssc.awaitTermination()
