FROM openjdk:11
RUN cd /opt && curl -OL http://archive.apache.org/dist/kafka/2.2.1/kafka_2.11-2.2.1.tgz && tar -zxvf kafka_2.11-2.2.1.tgz && rm kafka_2.11-2.2.1.tgz && \ 
curl -OL http://archive.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz && tar -zxvf zookeeper-3.4.6.tar.gz && rm zookeeper-3.4.6.tar.gz

WORKDIR /opt
COPY zoo.cfg /opt/zookeeper-3.4.6/conf
COPY server.properties /opt/kafka_2.11-2.2.1/config/
COPY start.sh /opt
CMD ["./start.sh"]

