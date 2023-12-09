# Run with:
# spark-submit --name ocr --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ocrjob.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import PIL
import requests
import json
import easyocr
import numpy as np

spark = SparkSession.builder.appName('ocr').getOrCreate()

model = easyocr.Reader(['en'])

@udf
def process(url):
    if not url.endswith((".jpg", ".jpeg", ".png", ".webp")):
        return ""
    try:
        image = PIL.Image.open(
            requests.get(url, stream=True).raw
        )
        open_cv_image = np.array(image.convert('RGB'))
        outputs = model.readtext(open_cv_image, detail=0, paragraph=True)
        text = "".join([phrase.lower() + ". " for phrase in outputs])
        return json.dumps(text) 
    except Exception:
        import traceback
        print(traceback.format_exc())
        return ""
    
schema = StructType([
    StructField("global_id", StringType()),
    StructField("author", StringType()),
    StructField("created_time", StringType()),
    StructField("desc", StringType()),
    StructField("score", IntegerType()),
    StructField("url", StringType()),
    StructField("source", StringType())
])

raw_kafka = (
  spark 
  .readStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "nifi:9092") 
  .option("subscribe", "nifi2ocr") 
  .load()
)

parsed_message = raw_kafka.select(
  from_json(col("value").cast("string"), schema).alias("message")
)

text = parsed_message.select(
  col("message.global_id").alias("global_id"),
  process(col("message.url")).alias("text")
)

kafka_message = text.select(
  to_json(struct(col("global_id"), col("text"))).alias("value")
)

ocr2analytics_ssc = (
    kafka_message
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers")
    .option("checkpointLocation","/tmp/ocr/checkpoint/analytics")
    .option("topic", "ocr2analytics")
    .start()
)

ocr2ner_ssc = (
    kafka_message
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "nifi:9092") 
    .option("checkpointLocation", "/tmp/ocr/checkpoint/ner")
    .option("topic", "ocr2ner")
    .start()
)

ocr2sentiment_ssc = (
    kafka_message
    .writeStream  
    .format("kafka") 
    .option("kafka.bootstrap.servers", "nifi:9092") 
    .option("checkpointLocation", "/tmp/ocr/checkpoint/sentiment")
    .option("topic", "ocr2sentiment")
    .start()
)

ocr2analytics_ssc.awaitTermination()
ocr2ner_ssc.awaitTermination()
ocr2sentiment_ssc.awaitTermination()