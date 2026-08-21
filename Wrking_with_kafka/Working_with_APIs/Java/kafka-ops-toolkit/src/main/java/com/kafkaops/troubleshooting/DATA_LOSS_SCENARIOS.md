# Data Loss Scenarios — Reference

Data loss in Kafka almost always traces back to one of these, in order of
how often they actually bite people in practice:

## 1. `acks=1` + leader failure before replication

With `acks=1`, the producer considers a write successful once the **leader**
has it — before followers replicate it. If the leader crashes before
followers catch up, those records are gone, even though the producer got a
success response.

**Fix:** `acks=all` + `min.insync.replicas >= 2` (see `AcksDurabilityDemo`
in this package for the producer-side config).

## 2. `unclean.leader.election.enable=true`

If all in-sync replicas are down, this setting lets an **out-of-sync**
replica become leader anyway, favoring availability over consistency — any
records that out-of-sync replica was missing are permanently lost.

**Fix:** Keep this `false` (the modern default) unless you've deliberately
decided availability matters more than zero data loss for that topic.

## 3. Consumer commits offset before processing completes

If a consumer commits (or auto-commits) an offset and then crashes before
finishing the side-effect (DB write, downstream publish, etc.), that record
is never reprocessed — it's "lost" from the consumer's point of view even
though it's still in Kafka.

**Fix:** Disable `enable.auto.commit`; commit only after processing
succeeds (at-least-once), or use transactional/idempotent writes if you
need exactly-once semantics.

## 4. Retention expiry before a slow/stuck consumer catches up

If a consumer group falls behind by more than `retention.ms` /
`retention.bytes`, the broker deletes the oldest segments out from under it
— those offsets are gone, and the consumer's next fetch jumps ahead
(or fails, depending on `auto.offset.reset`).

**Fix:** Alert on consumer lag well before it approaches retention limits
(see `LagTrendDiagnostic` in this package); size retention with your worst
realistic consumer downtime in mind, not just the happy path.

## 5. Replication factor of 1

With `replication.factor=1`, there is no replica to fail over to — losing
that one broker (disk failure, accidental termination) loses every message
on it permanently. This is the single most common root cause of "we lost a
topic" incidents in small/test clusters that got promoted to production
without revisiting this setting.

**Fix:** `replication.factor >= 3` for anything that matters, with
`min.insync.replicas=2` so you can still tolerate one broker down.

## 6. Producer buffer drop on `max.block.ms` timeout (silent in fire-and-forget code)

If you call `producer.send()` without checking the returned `Future` (or
without a callback), and the buffer fills up and the call eventually times
out and throws, fire-and-forget code never notices the exception — the
record is silently dropped from the application's perspective.

**Fix:** Always attach a callback (or check the `Future`) and log/alert on
send failures; never treat `producer.send()` as fire-and-forget in
production code.

---

### Why this is a reference doc, not a runnable demo

Reliably reproducing data loss requires actually killing a broker mid-write
on a multi-broker cluster, which isn't something to script against your
local single-broker setup. The configs referenced above
(`AcksDurabilityDemo`) are safe to run any time — they show you the
*producer-side levers*
