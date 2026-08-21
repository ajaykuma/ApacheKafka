package com.kafka.examples.streams.statestores;

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
 * Produces stock price ticks to feed StateStoreExamples.
 * Includes a deliberate >5% price drop on BMW to trigger an alert.
 *
 * HOW TO RUN (while StateStoreExamples is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.statestores.StockPriceProducer"
 */
public class StockPriceProducer {

    private static final Logger log = LoggerFactory.getLogger(StockPriceProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(KafkaConfig.TOPIC_STOCK_PRICES);

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            var random = new Random();

            // Starting prices and previous closes
            record TickerState(String ticker, double price, double prevClose) {}
            var tickers = new java.util.ArrayList<>(List.of(
                    new TickerState("BMW",  100.50, 100.00),
                    new TickerState("SAP",  178.20, 177.00),
                    new TickerState("SIE",   172.40, 171.50),
                    new TickerState("ALV",   247.80, 246.00)
            ));

            log.info("Publishing 40 stock price ticks across 4 tickers...");
            for (int round = 0; round < 10; round++) {

                for (int i = 0; i < tickers.size(); i++) {
                    var t = tickers.get(i);

                    // On round 7, inject a big drop for BMW (>5%) to trigger the alert
                    double newPrice;
                    if (round == 7 && t.ticker().equals("BMW")) {
                        newPrice = t.price() * 0.93; // -7% crash
                        log.warn(" Injecting BMW crash: {} → {}", t.price(), String.format("%.2f", newPrice));
                    } else {
                        // Normal random walk: ±1.5%
                        double change = (random.nextDouble() - 0.5) * 0.03;
                        newPrice = t.price() * (1 + change);
                    }

                    var stock = new StockPrice(t.ticker(), newPrice, t.prevClose(), System.currentTimeMillis());
                    producer.send(new ProducerRecord<>(
                            KafkaConfig.TOPIC_STOCK_PRICES,
                            t.ticker(),
                            JsonUtil.toJson(stock)
                    ), (meta, ex) -> {
                        if (ex == null)
                            log.info(" {} price={}", t.ticker(), String.format("%.2f", newPrice));
                    });

                    tickers.set(i, new TickerState(t.ticker(), newPrice, t.price()));
                }

                producer.flush();
                Thread.sleep(1500);
            }
            log.info(" Done. Check StateStoreExamples for interactive query output and alerts.");
        }
    }
}
