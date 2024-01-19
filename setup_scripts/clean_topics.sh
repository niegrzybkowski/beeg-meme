kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic cluster2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ner2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2ocr --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2vit --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2ner --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2sentiment --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic sentiment2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic vit2analytics --alter --add-config retention.ms=1
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic vit2cluster --alter --add-config retention.ms=1

sleep 1;

kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic cluster2analytics --alter --add-config retention.ms=8640000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ner2analytics --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2analytics --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2ocr --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic nifi2vit --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2analytics --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2ner --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic ocr2sentiment --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic sentiment2analytics --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic vit2analytics --alter --add-config retention.ms=86400000
kafka/bin/kafka-configs.sh --bootstrap-server kafka-0:9092 --topic vit2cluster --alter --add-config retention.ms=86400000