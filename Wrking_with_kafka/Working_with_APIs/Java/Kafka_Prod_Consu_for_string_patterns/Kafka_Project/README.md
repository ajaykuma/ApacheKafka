# Spring Kafka — Enterprise Patterns

Extends your existing 2 Spring Boot projects (`kafka-producer` on :8080,
`kafka-consumer` on :8081) with production-grade patterns, while keeping
your original `KafkaConfiguration`, `ProducerController`, `ConsumerController`
fully intact and working.

---

## How to integrate

### Producer project (port 8080)

Copy these files into your existing producer project, preserving the package structure:

```
src/main/java/com/example/kafka/kafkaproducer/
├── model/
│   └── OrderEvent.java
├── config/
│   ├── JsonProducerConfig.java
│   └── TransactionalProducerConfig.java
├── service/
│   └── OrderService.java
└── controller/
    └── OrderController.java
```

### Consumer project (port 8081)

```
src/main/java/com/example/kafka/kafkaconsumer/
├── model/
│   └── OrderEvent.java
├── config/
│   ├── JsonConsumerConfig.java
│   └── BatchConsumerConfig.java
└── listener/
    ├── OrderEventListener.java
    ├── BatchOrderListener.java
    └── DltMonitorListener.java
```

### Required pom.xml dependency (both projects)

Your existing `pom.xml` likely already has `spring-kafka` from Spring Initializr.
Confirm it includes:

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

Jackson is normally pulled in transitively by `spring-boot-starter-web`, so if
your producer/consumer already has that starter, you likely need no changes.

---

## What's new — Producer (5 patterns)

| Pattern | Class | Endpoint |
|---|---|---|
| JSON send | `OrderService.sendJsonOrder()` | `POST /orders/json` |
| Async + callback | `OrderService.sendOrderAsync()` | `POST /orders/async` |
| Custom partition key | `OrderService.sendOrderWithKey()` | `POST /orders/keyed` |
| Transactional (EOS) | `OrderService.sendOrdersTransactionally()` | `POST /orders/transactional` |
| Fire-and-forget | `OrderService.sendOrderFireAndForget()` | `POST /orders/fire-and-forget` |

### Test each endpoint

```bash
# Basic JSON send
curl -X POST "http://localhost:8080/orders/json?customerId=CUST-100&productId=PROD-A1&quantity=2&amount=149.99"

# Async with callback (check producer logs for success/failure)
curl -X POST "http://localhost:8080/orders/async?customerId=CUST-101&productId=PROD-B3"

# Custom partition key (key=PROD-A1 instead of customerId)
curl -X POST "http://localhost:8080/orders/keyed?customerId=CUST-100&productId=PROD-A1&key=PROD-A1"

# Transactional — successful commit
curl -X POST "http://localhost:8080/orders/transactional?customerId=CUST-102&productId=PROD-C7&fail=false"

# Transactional — simulated failure, should ABORT (check spring.orders.events
# with isolation.level=read_committed — this record should be invisible)
curl -X POST "http://localhost:8080/orders/transactional?customerId=CUST-103&productId=PROD-D2&fail=true"

# Fire and forget
curl -X POST "http://localhost:8080/orders/fire-and-forget?customerId=CUST-104&productId=PROD-E9"
```

On Windows without curl, use Postman or your browser's REST client extension,
or PowerShell:
```powershell
Invoke-WebRequest -Method POST -Uri "http://localhost:8080/orders/json?customerId=CUST-100&productId=PROD-A1"
```

---

## What's new — Consumer (3 patterns)

| Pattern | Class | Behaviour |
|---|---|---|
| Manual ack + retry + DLT | `OrderEventListener` | Validates, retries 3x on failure, then DLT |
| Batch processing | `BatchOrderListener` | Receives up to 50 records per invocation |
| DLT monitoring | `DltMonitorListener` | Watches `spring.orders.events.DLT` |

### Test retry + DLT behaviour

Send an order with `productId=FAIL` to trigger the validation failure path:

```bash
curl -X POST "http://localhost:8080/orders/json?customerId=CUST-100&productId=FAIL"
```

Watch the consumer logs — you should see:
```
Processing failed for orderId=...
[retry after 1 second]
Processing failed for orderId=...
[retry after 1 second]
Processing failed for orderId=...
DLT RECORD DETECTED — key=...
```

3 failed attempts, then the record lands on `spring.orders.events.DLT`,
picked up by `DltMonitorListener`.

### Test batch processing

Send several orders quickly:
```bash
for i in 1 2 3 4 5; do
  curl -X POST "http://localhost:8080/orders/json?customerId=CUST-10$i&productId=PROD-A1"
done
```

Both `OrderEventListener` (one at a time) and `BatchOrderListener` (batched)
will independently receive and process all 5 — they're in different consumer
groups, so it's a fan-out, same as your 2A example.

---

## Verifying with the Kafka CLI

Same approach as the plain-Java examples — check topics, offsets, and consumer
group lag:

```bash
kafka-topics.bat --bootstrap-server localhost:9092 --list

kafka-run-class.bat kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic spring.orders.events
kafka-run-class.bat kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic spring.orders.events.DLT
kafka-run-class.bat kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic spring.inventory.events

kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group order-processing-group
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group order-batch-processing-group
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group dlt-monitor-group
```

### Verify transaction isolation (EOS)

To see the difference between committed and aborted transactional writes,
use `kafka-console-consumer` with isolation level:

```bash
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic spring.orders.events ^
  --consumer-property isolation.level=read_committed --from-beginning --max-messages 10

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic spring.orders.events ^
  --consumer-property isolation.level=read_uncommitted --from-beginning --max-messages 10
```

The aborted order (sent with `fail=true`) should appear in `read_uncommitted`
but NOT in `read_committed` — same guarantee proven in the plain-Java EOS example.

---

## Key Differences From Plain Kafka Client Examples

| Concept | Plain Kafka Client | Spring Kafka |
|---|---|---|
| Sending a record | `producer.send(record, callback)` | `kafkaTemplate.send(topic, key, value)` returns `CompletableFuture` |
| Consuming | Manual `poll()` loop | `@KafkaListener` annotation, container manages polling |
| Manual commit | `consumer.commitSync()` | `Acknowledgment.acknowledge()` |
| Transactions | `beginTransaction()/commitTransaction()` | `@Transactional("kafkaTransactionManager")` |
| Error handling | Manual try/catch + DLT producer | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` (built-in) |
| Batch consuming | Loop over `poll()` results yourself | `setBatchListener(true)` + `List<ConsumerRecord<...>>` parameter |

Spring Kafka wraps the same underlying Kafka client concepts in declarative,
annotation-driven APIs — the Kafka-level guarantees (at-least-once delivery,
exactly-once transactions, partition-based ordering) are identical underneath.
