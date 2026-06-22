package com.kafka.examples.streams.basics;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Produces sensor readings to feed the KStream examples.
 * Produces a mix of normal, high-temp, and critical readings
 * so all branches of the topology are exercised.
 *
 * HOW TO RUN (while KStreamOperations is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.basics.SensorDataProducer"
 */
public class SensorDataProducer {

    private static final Logger log = LoggerFactory.getLogger(SensorDataProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_SENSOR_READINGS,
                "streams.sensor.humidity-alerts"
        );

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            var random    = new Random();
            var sensors   = List.of("SENS-001", "SENS-002", "SENS-003", "SENS-004", "SENS-005");
            var locations = List.of("FactoryFloor-A", "FactoryFloor-B", "ServerRoom-1", "Warehouse-North");

            // Predefined readings to exercise every branch of the topology
            var readings = List.of(
                // Normal: temp < 75 (filtered out by KStreamOperations)
                new SensorReading("SENS-001", "FactoryFloor-A",  62.3,  55.0, System.currentTimeMillis()),
                new SensorReading("SENS-002", "ServerRoom-1",    68.0,  40.0, System.currentTimeMillis()),
                // WARNING: 75 < temp < 90
                new SensorReading("SENS-003", "FactoryFloor-B",  78.5,  60.0, System.currentTimeMillis()),
                new SensorReading("SENS-004", "Warehouse-North", 82.1,  72.0, System.currentTimeMillis()),
                // CRITICAL: temp > 90
                new SensorReading("SENS-005", "ServerRoom-1",    94.7,  45.0, System.currentTimeMillis()),
                new SensorReading("SENS-001", "FactoryFloor-A",  91.3,  58.0, System.currentTimeMillis()),
                // High humidity: humidity > 80
                new SensorReading("SENS-002", "FactoryFloor-B",  70.0,  85.5, System.currentTimeMillis()),
                new SensorReading("SENS-003", "Warehouse-North", 65.0,  92.0, System.currentTimeMillis()),
                // Normal again
                new SensorReading("SENS-004", "FactoryFloor-A",  55.0,  50.0, System.currentTimeMillis()),
                new SensorReading("SENS-005", "ServerRoom-1",    71.2,  38.0, System.currentTimeMillis())
            );

            for (var reading : readings) {
                producer.send(new ProducerRecord<>(
                        KafkaConfig.TOPIC_SENSOR_READINGS,
                        reading.sensorId(),
                        JsonUtil.toJson(reading)
                ), (meta, ex) -> {
                    if (ex == null)
                        log.info(" Published: sensorId={} temp={} humidity={}",
                                reading.sensorId(), reading.temperature(), reading.humidity());
                });
                Thread.sleep(300);
            }
            producer.flush();
            log.info(" All sensor readings published.");
        }
    }
}
