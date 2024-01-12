#!/bin/bash

kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2vit" --replication-factor 1 --partitions 3
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2ocr"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "nifi2analytics"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "vit2cluster"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "vit2analytics"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "cluster2analytics"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2ner"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2sentiment"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ocr2analytics"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "ner2analytics"
kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "sentiment2analytics"
