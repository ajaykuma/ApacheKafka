#!/bin/bash
mkdir /tmp/zookeeper1
mkdir /tmp/zookeeper2
mkdir /tmp/zookeeper3
echo 1 > /tmp/zookeeper1/myid
echo 2 > /tmp/zookeeper2/myid
echo 3 > /tmp/zookeeper3/myid

nohup zookeeper-server-start.sh /usr/local/kafka/sncluster/zookeeper1.properties &
sleep 5s
nohup zookeeper-server-start.sh /usr/local/kafka/sncluster/zookeeper2.properties &
sleep 5s
nohup zookeeper-server-start.sh /usr/local/kafka/sncluster/zookeeper3.properties &
sleep 5s
nohup kafka-server-start.sh /usr/local/kafka/sncluster/server1.properties &
sleep 5s
nohup kafka-server-start.sh /usr/local/kafka/sncluster/server2.properties &
sleep 5s
nohup kafka-server-start.sh /usr/local/kafka/sncluster/server3.properties &
sleep 10s
echo stat | nc localhost 2181 | grep Mode
sleep 2s
echo stat | nc localhost 2182 | grep Mode
sleep 2s
echo stat | nc localhost 2183 | grep Mode  
sleep 10s
echo dump |nc localhost 2181 | grep brokers
sleep 5s
kafka-topics.sh --delete --topic Test1 --bootstrap-server localhost:9092
kafka-topics.sh --describe --bootstrap-server localhost:9092
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Test1 --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:9092 --list

