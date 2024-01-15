# Run with:
# spark-submit --name ocr --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ocrjob.py
# Install tesserocr by:
# 1. Download `tesserocr-{version}.whl` from https://github.com/simonflueckiger/tesserocr-windows_build/releases
# 2. Run `pip install tesserocr-{version}.whl`
# 3. Download `eng.traineddata` from https://github.com/tesseract-ocr/tessdata/tree/main
# 4. Place `eng.traineddata` in the working directory
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import PIL
import requests
import json
import tesserocr
import numpy as np
from google.cloud import storage
import re

spark = SparkSession.builder.appName('ocr').getOrCreate()

def get_image(url):
    if not url.endswith((".jpg", ".jpeg", ".png", ".webp")):
        return None
    if url[:5] == "gs://":
        url_trimmed = url.replace("gs://", "")
        bucket_name, blob_name = url_trimmed.split("/", 1)
        storage_client = storage.Client()
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        result = blob.download_as_bytes()
        contents = io.BytesIO(result)
    else:
        contents = requests.get(url, stream=True).raw
    
    image = PIL.Image.open(contents)
    return image   

@udf
def process(url):
    print("[UDF] Processing ", url)
    if not url.endswith((".jpg", ".jpeg", ".png", ".webp")):
        return ""
    try:
        image = get_image(url)
        text = tesserocr.image_to_text(image.convert('RGB'))
        text = text.lower().strip().replace('\n',' ')
        text = re.sub(r"( )\1+",r". ", text)
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
  .option("kafka.bootstrap.servers", "kafka-0:9092") 
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
    .option("kafka.bootstrap.servers", "kafka-0:9092")
    .option("checkpointLocation","/tmp/ocr/checkpoint/analytics")
    .option("topic", "ocr2analytics")
    .start()
)

ocr2ner_ssc = (
    kafka_message
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "kafka-0:9092") 
    .option("checkpointLocation", "/tmp/ocr/checkpoint/ner")
    .option("topic", "ocr2ner")
    .start()
)

ocr2sentiment_ssc = (
    kafka_message
    .writeStream  
    .format("kafka") 
    .option("kafka.bootstrap.servers", "kafka-0:9092") 
    .option("checkpointLocation", "/tmp/ocr/checkpoint/sentiment")
    .option("topic", "ocr2sentiment")
    .start()
)

ocr2analytics_ssc.awaitTermination()
ocr2ner_ssc.awaitTermination()
ocr2sentiment_ssc.awaitTermination()