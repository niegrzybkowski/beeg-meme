# Run with:
# spark-submit --name sentiment --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 sentimentjob.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformers import pipeline
import json

spark = SparkSession.builder.appName('sentiment').getOrCreate()

model_path = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
sentiment = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)

@udf
def process(text):
    print("[UDF] Processing", text)
    try:
        sent_results = sentiment(text)
        #sent_results = {
        #    'sentiment':sent_results[0]['label'],
        #    'sentiment_score':sent_results[0]['score']
        #}
        return json.dumps(sent_results)
    except Exception:
        import traceback
        print(traceback.format_exc())
        return ""

schema = StructType([
    StructField("global_id", StringType()),
    StructField("text", StringType())
])

raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
  .option("subscribe", "ocr2sentiment") 
  .load()
)

parsed_message = raw_kafka.select(
  from_json(col("value").cast("string"), schema).alias("message")
)

sents = parsed_message.select(
  col("message.global_id").alias("global_id"),
  process(col("message.text")).alias("sentiments")
)

kafka_message = sents.select(
  to_json(struct(col("global_id"), col("sentiments"))).alias("value")
)

sentiment2analytics_ssc = (
    kafka_message
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "kafka-0:9092") 
    .option("checkpointLocation", "/tmp/sentiment/checkpoint/analytics")
    .option("topic", "sentiment2analytics")
    .start()
)

sentiment2analytics_ssc.awaitTermination()