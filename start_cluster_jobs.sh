spark-submit --name vit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[4] \
    vitjob.py &> ~log/vit.log &

    --master spark://kafka-0:7077 \
    --num-executors 3 \
    --executor-memory 4G \
    --executor-cores 1 \
    --total-executor-cores 3 \

spark-submit --name ocr \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[4] \
    ocrjob.py &> ~/log/sentiment.log & 


spark-submit --name ner \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[2] \
    nerjob.py &> ~/log/ner.log & 

spark-submit --name cluster \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[1] \
    clusterjob.py &> /var/log/cluster.log  & 

spark-submit --name sentiment \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master spark://kafka-0:7077 \
    --num-executors 1 \
    --executor-memory 4G \
    --executor-cores 1 \
    --total-executor-cores 1 \
    sentimentjob.py &> /var/log/sentiment.log & 

spark-submit --name analytics \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --master local[1] \
    analyticsjob.py &> ~/log/analytics.log & 