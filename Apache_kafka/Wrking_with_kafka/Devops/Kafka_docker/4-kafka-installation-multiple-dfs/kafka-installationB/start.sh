#!/bin/bash
mkdir -p /usr/local/zookeeper/state/zookeeper
echo 1 > /usr/local/zookeeper/state/zookeeper/myid
mkdir -p /usr/local/kafka/kafka-logs

zookeeper-3.4.6/bin/zkServer.sh start
sleep 5
kafka_2.11-2.2.1/bin/kafka-server-start.sh kafka_2.11-2.2.1/config/server.properties
