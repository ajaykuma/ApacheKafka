FROM openjdk:11
RUN cd /opt && curl -OL http://archive.apache.org/dist/kafka/2.2.1/kafka_2.11-2.2.1.tgz && tar -zxvf kafka_2.11-2.2.1.tgz && rm kafka_2.11-2.2.1.tgz
WORKDIR /opt/kafka_2.11-2.2.1/
COPY server.properties /opt/kafka_2.11-2.2.1/config/
COPY start.sh /opt/kafka_2.11-2.2.1/
CMD ["./start.sh"]


