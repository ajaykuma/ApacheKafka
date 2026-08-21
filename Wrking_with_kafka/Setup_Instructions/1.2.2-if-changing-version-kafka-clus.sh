#!/bin/bash
cd /usr/local/
sudo unlink kafka

#remember to download and untar kafka_2.13-2.8.1.tgz in /usr/local location & change permissions to 'hdu'
sudo ln -s kafka_2.13-2.8.1 kafka
sudo chown -R hdu:hdu kafka*

#config changes to make sure different ports since we use same node for multiple processes of zk
cp -r kafka_2.11-2.2.1/sncluster/ kafka/
sed -i 's/server.2=localhost:2888:3888/server.2=localhost:2889:3889/g' /usr/local/kafka/sncluster/zookeeper1.properties 
sed -i 's/server.3=localhost:2888:3888/server.2=localhost:2890:3890/g' /usr/local/kafka/sncluster/zookeeper1.properties 
echo 4lw.commands.whitelist=* >> /usr/local/kafka/sncluster/zookeeper1.properties
sed -i 's/server.2=localhost:2888:3888/server.2=localhost:2889:3889/g' /usr/local/kafka/sncluster/zookeeper2.properties     
sed -i 's/server.3=localhost:2888:3888/server.2=localhost:2890:3890/g' /usr/local/kafka/sncluster/zookeeper2.properties
echo 4lw.commands.whitelist=* >> /usr/local/kafka/sncluster/zookeeper2.properties
sed -i 's/server.2=localhost:2888:3888/server.2=localhost:2889:3889/g' /usr/local/kafka/sncluster/zookeeper3.properties     
sed -i 's/server.3=localhost:2888:3888/server.2=localhost:2890:3890/g' /usr/local/kafka/sncluster/zookeeper3.properties
echo 4lw.commands.whitelist=* >> /usr/local/kafka/sncluster/zookeeper3.properties
cat /usr/local/kafka/sncluster/zookeeper1.properties
cat /usr/local/kafka/sncluster/zookeeper2.properties
cat /usr/local/kafka/sncluster/zookeeper3.properties

#Note** Once we have checked and verified zookeeper*.properties & server*.proprties, we can use '1.2.1-setup-kafka-clus.sh' to start our cluster..

