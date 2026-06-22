# Kafka & Kafka Streams — Enterprise Examples

Working Java 21 + Maven examples for every pattern in **Sections 2 and 3** of the
Consulting Guide. Each class is self-contained, fully commented, and runnable against
a local Kafka on `localhost:9092`.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java | 21 |
| Maven | 3.8+ |
| Apache Kafka | 3.x running on `localhost:9092` |

### Start Kafka locally (if not already running)

```bash
# Download Kafka 3.7 from https://kafka.apache.org/downloads
# Then start with KRaft (no ZooKeeper needed):

bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

---

## Build

```bash
mvn clean compile -q
```

---

## Run any example

```bash
mvn exec:java -Dexec.mainClass="<fully.qualified.ClassName>"
```

---

## Project Structure

```
src/main/java/com/kafka/examples/
│
├── config/
│   └── KafkaConfig.java              # Central config: bootstrap, topic names, props
│
├── common/
│   └── JsonUtil.java                 # JSON helpers + all domain event record types
│
├── archetypes/                       # SECTION 2 — Enterprise Usage Patterns
│   ├── a_eventdriven/
│   │   ├── EventDrivenProducer.java  # 2A: Publish domain events (account, vehicle, order)
│   │   └── EventDrivenConsumer.java  # 2A: Multi-group consumers + DLT pattern
│   │
│   ├── b_streaming/
│   │   ├── OrderEnrichmentPipeline.java  # 2B: KStream-KTable join enrichment pipeline
│   │   └── EnrichmentVerifier.java       # 2B: Consumer to verify enriched output
│   │
│   ├── c_ingestion/
│   │   └── IngestionPipeline.java    # 2C: REST-proxy-style ingestion + validation + DLT
│   │
│   └── d_sharing/
│       └── AsyncDataSharing.java     # 2D: Warehouse + shipment async decoupling
│
└── streams/                          # SECTION 3 — Kafka Streams Deep Dive
    ├── basics/
    │   ├── KStreamOperations.java    # 3.2: filter, map, flatMap, branch, merge, peek
    │   └── SensorDataProducer.java   # feeds KStreamOperations
    │
    ├── joins/
    │   ├── JoinExamples.java         # 3.2: KStream-KStream, KStream-KTable, KStream-GlobalKTable
    │   └── JoinDataProducer.java     # feeds JoinExamples
    │
    ├── windowing/
    │   ├── WindowingExamples.java    # 3.3: Tumbling, Hopping, Session windows + suppress()
    │   └── ClickEventProducer.java   # feeds WindowingExamples
    │
    └── statestores/
        ├── StateStoreExamples.java   # 3.4: Custom aggregation + Interactive Queries
        └── StockPriceProducer.java   # feeds StateStoreExamples

src/test/java/com/kafka/examples/
└── TopologyTests.java                # Unit tests with TopologyTestDriver (no Kafka needed)
```

---

## Examples — Section 2: Enterprise Patterns

### 2A — Event-Driven Microservices

Demonstrates domain event publishing and multi-consumer group subscription.
Each consumer group gets ALL messages independently.

```bash
# Terminal 1: publish domain events
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.a_eventdriven.EventDrivenProducer"

# Terminal 2: consume (simulates 3 microservices reacting to the same events)
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.a_eventdriven.EventDrivenConsumer"
```

**What you'll see:**
- GDPR service, notification service, and inventory service each receive the same events
- A deliberately bad record is routed to the `.DLT` dead-letter topic
- Manual offset commit after successful processing

**Key code:** `KafkaConfig.producerProps()` has `enable.idempotence=true` and `acks=all`

---

### 2B — Data Streaming & Enrichment

A full Kafka Streams KStream-KTable join pipeline. Orders are enriched with customer profiles.

```bash
# Terminal 1: run the enrichment pipeline (also seeds test data)
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.b_streaming.OrderEnrichmentPipeline"

# Terminal 2: verify enriched output
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.b_streaming.EnrichmentVerifier"
```

**What you'll see:**
- Customer profiles loaded into a KTable (local RocksDB state store)
- Each incoming order joined with the customer's tier and region
- Order for unknown customer `CUST-999` passes through with default profile (left join)
- Enriched JSON on `enrichment.orders.enriched` topic

---

### 2C — Ingestion & Distribution Layer

Simulates ServiceNow REST Proxy ingestion with validation, routing by category, and DLT.

```bash
# Runs producer and consumer together:
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.c_ingestion.IngestionPipeline"
```

**What you'll see:**
- 30 incidents ingested in 3 batches
- 2 deliberately malformed records → routed to `integration.servicenow.inbound.DLT`
- Valid records routed by category (INCIDENT / CHANGE / PROBLEM)
- Summary stats printed at end

---

### 2D — Async Data Sharing

Four virtual-thread "microservices" sharing warehouse and shipment events. Each service
has its own consumer group and processes events independently.

```bash
mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.d_sharing.AsyncDataSharing"
```

**What you'll see:**
- Warehouse inventory events consumed by both `fulfillment-service` and `reporting-service`
- Shipment status events consumed by `notification-service` (only on SHIPPED/DELIVERED)
  and `customer-portal-service` (on every status change)
- Events in correct partition order per shipment (PICKED → PACKED → SHIPPED → DELIVERED)

---

## Examples — Section 3: Kafka Streams Deep Dive

### 3.2 — KStream Core Operations

`filter`, `filterNot`, `mapValues`, `map`, `flatMap`, `branch`, `merge`, `peek`, `selectKey`

```bash
# Terminal 1: start the topology
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.basics.KStreamOperations"

# Terminal 2: push sensor data
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.basics.SensorDataProducer"
```

**What you'll see:**
- Readings below 75°C silently filtered out
- 75–90°C → WARNING alert on `streams.sensor.alerts`
- Above 90°C → CRITICAL alert on `streams.sensor.alerts`
- High-humidity readings rekeyed by location (selectKey) and routed separately
- flatMap producing multiple output records from one input

---

### 3.2 — All Three Join Types

KStream-KStream (windowed), KStream-KTable (non-windowed), KStream-GlobalKTable (no repartition)

```bash
# Terminal 1: start join topology
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.joins.JoinExamples"

# Terminal 2: push test data
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.joins.JoinDataProducer"
```

**What you'll see:**
- `[KStream-KStream]` U-001 click + purchase within 5-min window → CONVERSION
- `[KStream-KTable]` All clicks enriched with user theme/language preferences
- `[KStream-GlobalKTable]` Clicks enriched with region data without repartitioning

**Key difference explained in logs:** GlobalKTable join fires without a `selectKey()` rekey step.

---

### 3.3 — Windowing (Tumbling / Hopping / Session)

```bash
# Terminal 1: start windowing topology
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.windowing.WindowingExamples"

# Terminal 2: push click events with deliberate timing gaps
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.windowing.ClickEventProducer"
```

**What you'll see:**
- `[TUMBLING 10s]` Window count emitted after window closes (via `suppress()`)
- `[HOPPING 30s/10s]` Same clicks appear in multiple overlapping windows
- `[SESSION gap=15s]` Session count emitted after 20s pause; new session starts

**Tip:** watch the timestamps in the log — tumbling windows appear in strict 10s blocks.

---

### 3.4 — State Stores & Interactive Queries

```bash
# Terminal 1: start topology with interactive query loop
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.statestores.StateStoreExamples"

# Terminal 2: push stock price ticks
mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.statestores.StockPriceProducer"
```

**What you'll see:**
- Custom `aggregate()` building `StockStats` (min/max/latest/count) per ticker in RocksDB
- Interactive query printing store contents every 5 seconds (simulating a REST endpoint)
- BMW deliberately crashes 7% on round 7 → `PRICE DROP ALERT` on `streams.stock.alerts`

---

## Unit Tests (no Kafka required)

Tests use `TopologyTestDriver` — runs the full topology in-memory with no broker needed.

```bash
mvn test
```

| Test class | Covers |
|---|---|
| `OrderEnrichmentTests` | KTable join, left join fallback, KTable update semantics |
| `KStreamOperationTests` | filter/branch — normal, WARNING, CRITICAL, mixed |
| `WindowingTests` | Tumbling window close, session gap close |
| `StreamStreamJoinTests` | Within-window join, outside-window no-join, KTable enrichment |

---

## Configuration

All tunable values are in `KafkaConfig.java`:

```java
public static final String BOOTSTRAP_SERVERS = "localhost:9092";  // change if needed
```

Producer best practices already applied:
- `enable.idempotence = true`
- `acks = all`
- `compression.type = lz4`
- Manual offset commit on consumers

---

## Topics Created Automatically

All examples call `KafkaConfig.createTopicsIfAbsent(...)` on startup.
Topics are created with **3 partitions, replication factor 1** (suitable for local single-broker).

To list all topics after running examples:
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

To inspect a topic:
```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic enrichment.orders.enriched \
  --from-beginning
```
