from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import io
import time


spark = SparkSession.builder.appName('analytics').getOrCreate()

### nifi ###
nifi_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "localhost:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "nifi2analytics") 
  .load()
)

nifi_schema = StructType([
    StructField("global_id", StringType()),
    StructField("author", StringType()),
    StructField("created_time", TimestampType()), # !!!! processing time or event time?
    StructField("desc", StringType()),
    StructField("score", IntegerType()),
    StructField("url", StringType()),
    StructField("source", StringType()),
#    StructField("timestamp", TimestampType()),
])
nifi_df = nifi_raw_df.select(
  from_json(col("value").cast("string"), nifi_schema).alias("message")
)

nifi_df_ts = nifi_df.select(
  col("message.global_id").alias("global_id"),
  col("message.author").alias("author"),
  col("message.created_time").alias("created_time"),
  col("message.desc").alias("desc"),
  col("message.score").alias("score"),
  col("message.url").alias("url"),
  col("message.source").alias("source"),
#  col("message.timestamp").alias("timestamp"),
)


### VIT ###
vit_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "vit2analytics") 
  .load()
)

vit_schema = StructType([
    StructField("global_id", StringType()),
    StructField("embeddings", StringType()),
    StructField("timestamp", TimestampType())
])

vit_df = vit_raw_df.select(
  from_json(col("value").cast("string"), vit_schema).alias("message")
)

vit_df_ts = vit_df.select(
  col("message.global_id").alias("global_id"),
  col("message.embeddings").alias("embeddings"),
  col("message.timestamp").alias("timestamp")
)


### Cluster ###
cluster_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "cluster2analytics") 
  .load()
)

cluster_schema = StructType([
    StructField("global_id", StringType()),
    StructField("cluster", StringType()),
    StructField("timestamp", TimestampType())
])

cluster_df = cluster_raw_df.select(
  from_json(col("value").cast("string"), cluster_schema).alias("message")
)

cluster_df_ts = cluster_df.select(
  col("message.global_id").alias("global_id"),
  col("message.cluster").alias("cluster"),
  col("message.timestamp").alias("timestamp")
)


### OCR ###
ocr_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "ocr2analytics") 
  .load()
)

ocr_schema = StructType([
    StructField("global_id", StringType()),
    StructField("text", StringType()),
    StructField("timestamp", TimestampType())
])

ocr_df = ocr_raw_df.select(
  from_json(col("value").cast("string"), ocr_schema).alias("message")
)

ocr_df_ts = ocr_df.select(
  col("message.global_id").alias("global_id"),
  col("message.text").alias("text"),
  col("message.timestamp").alias("timestamp")
)


### NER ###
ner_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "ner2analytics") 
  .load()
)

ner_schema = StructType([
    StructField("global_id", StringType()),
    StructField("entities", StringType()),
    StructField("timestamp", TimestampType())
])

ner_df = ner_raw_df.select(
  from_json(col("value").cast("string"), ner_schema).alias("message")
)

ner_df_ts = ner_df.select(
  col("message.global_id").alias("global_id"),
  col("message.entities").alias("entities"),
  col("message.timestamp").alias("timestamp")
)


### Sentiment ###
sent_raw_df = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("startingOffsets", "latest")
  .option("subscribe", "sentiment2analytics") 
  .load()
)

sent_schema = StructType([
    StructField("global_id", StringType()),
    StructField("sentiments", StringType()),
    StructField("timestamp", TimestampType())
])

sent_df = sent_raw_df.select(
  from_json(col("value").cast("string"), sent_schema).alias("message")
)

sent_df_ts = sent_df.select(
  col("message.global_id").alias("global_id"),
  col("message.sentiments").alias("sentiments"),
  col("message.timestamp").alias("timestamp")
)


### Join steams ###
threshold_sec = 60
df_basic = nifi_df_ts.withWatermark("created_time", f"{threshold_sec} seconds")
df_vit = vit_df_ts.withWatermark("timestamp", f"{threshold_sec} seconds")
df_cluster = cluster_df_ts.withWatermark("timestamp", f"{threshold_sec} seconds")
df_ocr = ocr_df_ts.withWatermark("timestamp", f"{threshold_sec} seconds")
df_ner = ner_df_ts.withWatermark("timestamp", f"{threshold_sec} seconds")
df_sent = sent_df_ts.withWatermark("timestamp", f"{threshold_sec} seconds")

### Rename columns ###
col_names = ['global_id', 'timestamp']
for col_name in ['global_id']: # df_basic.columns:
    df_basic = df_basic.withColumnRenamed(col_name, f"basic_{col_name}")
for col_name in col_names: # df_vit.columns:
    df_vit = df_vit.withColumnRenamed(col_name, f"vit_{col_name}")
for col_name in col_names: # df_cluster.columns:
    df_cluster = df_cluster.withColumnRenamed(col_name, f"cluster_{col_name}")
for col_name in col_names: # df_ocr.columns:
    df_ocr = df_ocr.withColumnRenamed(col_name, f"ocr_{col_name}")
for col_name in col_names: # df_ner.columns:
    df_ner = df_ner.withColumnRenamed(col_name, f"ner_{col_name}")
for col_name in col_names: # df_sent.columns:
    df_sent = df_sent.withColumnRenamed(col_name, f"sent_{col_name}")


### Drop duplicates ###
#df_basic = df_basic.dropDuplicates()
#df_vit = df_vit.dropDuplicates()
#df_cluster = df_cluster.dropDuplicates()
#df_ocr = df_ocr.dropDuplicates()
#df_ner = df_ner.dropDuplicates()
#df_sent = df_sent.dropDuplicates()

### join ###

# + VIT
df = df_basic.join(
        df_vit,
        expr("""
            vit_global_id = basic_global_id AND
            vit_timestamp >= created_time AND
            vit_timestamp <= created_time + interval 1 minute
            """),
        "leftOuter"                 
        )

# + CLUSTER
df = df.join(
        df_cluster,
        expr("""
            cluster_global_id = basic_global_id AND
            cluster_timestamp >= created_time AND
            cluster_timestamp <= created_time + interval 1 minute
            """),
        "leftOuter"                 
        )
# + OCR
df = df.join(
        df_ocr,
        expr("""
            ocr_global_id = basic_global_id AND
            ocr_timestamp >= created_time AND
            ocr_timestamp <= created_time + interval 1 minute
            """),
        "leftOuter"                 
        )
# + NER
df = df.join(
        df_ner,
        expr("""
            ner_global_id = basic_global_id AND
            ner_timestamp >= created_time AND
            ner_timestamp <= created_time + interval 1 minute
            """),
        "leftOuter"                 
        )
# + OCR
df = df.join(
        df_sent,
        expr("""
            sent_global_id = basic_global_id AND
            sent_timestamp >= created_time AND
            sent_timestamp <= created_time + interval 1 minute
            """),
        "leftOuter"                 
        )


#df = df.dropDuplicates()

kafka_message = df.select(
    to_json(
        struct(
            col("basic_global_id").alias("global_id"),
            col("author"),
            col("created_time"),
            col("desc"),
            col("score"),
            col("url"),
            col("source"),
            col("embeddings"),
            col("cluster"),
            col("text"), 
            col("entities"),
            col("sentiments"),
#            col("basic_timestamp")
            )
    ).alias("value")
)

query = (
    kafka_message
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-0:9092")
    .option("checkpointLocation", "/tmp/analytics/checkpoint/analytics")
    .option("topic", "test")
#    .trigger(processingTime='10 seconds')
    .start()
)

query.awaitTermination()