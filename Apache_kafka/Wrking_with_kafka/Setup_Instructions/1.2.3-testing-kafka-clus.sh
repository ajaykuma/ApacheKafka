#!/bin/bash
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --describe --bootstrap-server localhost:9092
sleep 2s
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Topic1 --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Topic2 --partitions 1 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Topic3 --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic BankData --partitions 3 --replication-factor 3
sleep 2s
kafka-topics.sh --bootstrap-server localhost:9092 --list
sleep 2s
kafka-console-producer.sh --topic BankData --broker-list localhost:9092 < /home/hdu/Bank_full.csv
sleep 2s

kafka-console-consumer.sh --topic BankData --bootstrap-server localhost:9092 --from-beginning --max-messages 5

kafka-console-consumer.sh --topic BankData --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property print.offset=true --property print.partition=true

kafka-console-consumer.sh --topic BankData --bootstrap-server localhost:9092 --from-beginning --max-messages 5
