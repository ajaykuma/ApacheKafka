# kafka-ops-toolkit

Standalone Java 21 / Maven project covering Kafka **production-readiness**
topics: Monitoring, Performance Tuning, Troubleshooting, and Security.
Separate from the existing producer/consumer/streams example apps — this is
focused on "running Kafka," not "building on Kafka."

Plain Java (no Spring Boot) so every class is a single runnable `main()` —
easy to point at your local broker and run directly.

## Setup

```bash
mvn clean compile
```

All commands below assume a local broker at `localhost:9092`.

## 1. Monitoring / Observability

| Class | What it shows |
|---|---|
| `ClusterHealthMonitor` | Broker list, controller, per-partition ISR health |
| `ConsumerLagMonitor` | Per-partition + total lag for a consumer group |
| `ClientMetricsPoller` | Live client-side metrics (the same numbers a JMX/Prometheus exporter would expose) |

```bash
mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ClusterHealthMonitor \
  -Dexec.args="localhost:9092"

mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ConsumerLagMonitor \
  -Dexec.args="localhost:9092 my-group"

mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ClientMetricsPoller \
  -Dexec.args="localhost:9092 my-topic my-group"
```

## 2. Performance Tuning

| Class | What it shows |
|---|---|
| `ProducerTuningDemo` | Low-latency vs high-throughput producer config, timed side-by-side |
| `ConsumerTuningDemo` | fetch.min.bytes / max.poll.records tradeoffs, timed side-by-side |
| `PartitionSizingAdvisor` | Back-of-envelope partition-count calculator |

```bash
mvn exec:java -Dexec.mainClass=com.kafkaops.performance.ProducerTuningDemo \
  -Dexec.args="localhost:9092 perf-test-topic 10000"

mvn exec:java -Dexec.mainClass=com.kafkaops.performance.ConsumerTuningDemo \
  -Dexec.args="localhost:9092 perf-test-topic"

mvn exec:java -Dexec.mainClass=com.kafkaops.performance.PartitionSizingAdvisor \
  -Dexec.args="100 10 20 4"
```

## 3. Troubleshooting

| Class | What it shows |
|---|---|
| `RebalanceTriggerDemo` | Reproduces a `max.poll.interval.ms`-triggered rebalance on purpose |
| `LagTrendDiagnostic` | Samples lag over time and classifies trend (stable / stuck / growing / recovering) |
| `BrokerFailureChecker` | Detects leaderless/under-replicated partitions and down-broker symptoms |
| `AcksDurabilityDemo` | Safe-to-run demo of `acks=all` + idempotence |
| `DATA_LOSS_SCENARIOS.md` | Reference doc — the 6 most common real-world data-loss causes and fixes |

```bash
mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.RebalanceTriggerDemo \
  -Dexec.args="localhost:9092 perf-test-topic"

mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.LagTrendDiagnostic \
  -Dexec.args="localhost:9092 my-group 5 3000"

mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.BrokerFailureChecker \
  -Dexec.args="localhost:9092"

mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.AcksDurabilityDemo \
  -Dexec.args="localhost:9092 perf-test-topic"
```

## 4. Security

**Conceptual + fully-commented config templates only — nothing here connects
with SSL/SASL enabled, since that requires reconfiguring the broker with
real certs/credentials first.** Each `main()` just prints the configs so you
can read them as reference and copy/adapt when you're ready to test for real.

| Class | What it shows |
|---|---|
| `SslConfigTemplates` | One-way TLS and mutual TLS (mTLS) client configs + broker-side reference |
| `SaslConfigTemplates` | SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER client configs |
| `AclReference` | `kafka-acls.sh` CLI examples + role-based access (RBAC) approaches |
| `EncryptionAndHardeningNotes` | Encryption-at-rest options + a secure-configuration checklist |

```bash
mvn exec:java -Dexec.mainClass=com.kafkaops.security.SslConfigTemplates
mvn exec:java -Dexec.mainClass=com.kafkaops.security.SaslConfigTemplates
mvn exec:java -Dexec.mainClass=com.kafkaops.security.AclReference
mvn exec:java -Dexec.mainClass=com.kafkaops.security.EncryptionAndHardeningNotes
```

## Notes

- Run `mvn clean compile` yourself once
  you pull this down locally with normal network access.
- `perf-test-topic` / `my-topic` / `my-group` are placeholders — swap in
  whatever topics/groups exist on your local broker, or create
  `perf-test-topic` fresh (`kafka-topics.sh --create --topic perf-test-topic
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092`).
