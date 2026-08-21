#!/bin/bash
mkdir -p /tmp/zookeeper
echo 1 > /tmp/zookeeper/myid
mkdir -p /tmp/kafka-logs

zookeeper-3.4.6/bin/zkServer.sh start
sleep 5
kafka_2.11-2.2.1/bin/kafka-server-start.sh kafka_2.11-2.2.1/config/server.properties
