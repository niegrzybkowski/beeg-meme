# Run with:
# spark-submit --name ner --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 nerjob.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
import json

NER_THRESHOLD = 0.75

spark = SparkSession.builder.appName('ner').getOrCreate()

model_path = "Babelscape/wikineural-multilingual-ner"
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForTokenClassification.from_pretrained(model_path)
ner = pipeline("ner", model=model, tokenizer=tokenizer, grouped_entities=True)

@udf
def process(text):
    print("[UDF] Processing", text)
    try:
        ner_results = ner(text)
        ner_results = [res for res in ner_results if res['score'] > NER_THRESHOLD]
        for i in range(len(ner_results)):
            ner_results[i]['score'] = str(ner_results[i]['score'])
        return json.dumps(ner_results) 
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
  .option("kafka.bootstrap.servers", "nifi:9092") 
  .option("subscribe", "ocr2ner") 
  .load()
)

parsed_message = raw_kafka.select(
  from_json(col("value").cast("string"), schema).alias("message")
)

nes = parsed_message.select(
  col("message.global_id").alias("global_id"),
  process(col("message.text")).alias("entities")
)

kafka_message = nes.select(
  to_json(struct(col("global_id"), col("entities"))).alias("value")
)

ner2analytics_ssc = (
    kafka_message
    .writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "nifi:9092") 
    .option("checkpointLocation", "/tmp/ner/checkpoint/analytics")
    .option("topic", "ner2analytics")
    .start()
)

ner2analytics_ssc.awaitTermination()