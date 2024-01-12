#!/bin/bash

/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2vit" --replication-factor 1 --partitions 3
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2ocr"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2analytics"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "vit2cluster"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "vit2analytics"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "cluster2analytics"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2ner"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2sentiment"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2analytics"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ner2analytics"
/home/nifi/kafka_2.13-3.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "sentiment2analytics"
