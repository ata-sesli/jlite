# Replica lag and durability bottleneck

The subsequent [concurrent ACK experiment](replica-acks.md) was measured and not
adopted; the serial receiver described here is the current implementation.

Serial replica ACK confirmation is the main receiver bottleneck. The remaining
200 writes/s limit is the durable broker/storage path in this Fedora setup.
The isolated broker confirmed only 200–201 writes/s; a single append-and-fsync
loop managed 213 operations/s on the same volume. The complete workload adds
owner commits, replica commits and consumer state to that storage workload.
Increasing the publisher's async window does not remove those disk syncs.

This investigation changes only test code and records measurements. Production
sources match the previous continuous-pipeline source manifest. Zova WAL/FULL,
JetStream file storage with `sync_interval: always`, authenticated connections,
ordered publication, retained-history checks and atomic checkpoints remain intact.

## Replica stage measurements

The profiled two-minute runs offered 24,000 writes at 200/s with both replicas
active. Runtime profiles and forced GC make these runs diagnostic evidence,
not a throughput comparison. The old publisher uses window barriers; the current
publisher refills continuously.

| Ingestion-stage measurement | Window barriers | Continuous refill |
| --- | --- | --- |
| Mean explicit replica `DoubleAck` response | 6.00–6.02 ms | 12.02–12.16 ms |
| Replica cycle time spent confirming serial ACKs | 67.7–67.9% | 67.5–67.7% |
| Local application and adjacent overhead | 24.2–24.5% | 9.2–9.3% |
| Retained-history reads | 0.8% | 2.4–2.5% |
| Stream Info requests | 0.9–1.0% | 10.1–10.8% |
| Applied writes during ingestion, per replica | 13,500–13,501 | 6,675–6,728 |

With continuous refill, observed replica ACK response time roughly doubles,
and the serial receiver loop makes that delay cumulative. SQLite application
and history validation are not the largest measured stages. This rejects the
initial hypothesis that repeated history reads explain most replica lag.

An ACK timer includes request/reply transport, server processing, contention and
scheduling. It is not a disk-sync timer. In pinned NATS 2.15.0, stream storage
synchronizes while holding its message-block lock. Consumer ACK handling updates
consumer state and enqueues a reply; it does not explicitly wait for a separate
consumer-state fsync on each ACK. The measurements do not apportion ACK delay
among every server queue and lock.

Greater contention from continuous broker writes is consistent with these
profiles and stage timings, but the exact division between queueing and lock
waits remains unmeasured. The controlled ACK experiment below establishes that
serial confirmation itself limits replica progress.

`ApplyAndLocal` measures the interval from fetch completion to the first ACK call,
including decoding, validation and the atomic replica commit. It includes small
local overhead and is not an isolated SQLite syscall measurement. Phase attribution
uses operation completion time, so a boundary-crossing cycle can straddle ingestion.

## Controlled receiver experiments

Unprofiled controls use the same current publisher, offered rate, durability and
fresh storage. The test-only `parallel-acks` mode starts every explicit DoubleAck
in a fetched group after the real replica transaction commits, and waits for every
confirmation. It uses the existing batch ACK context and timeout. It neither
acknowledges before commit nor uses AckAll. The `batch32` mode increases only
receiver fetch/transaction bounds from eight to 32 messages; wire limits and
the owner's publication window stay unchanged. Fetch wait remains 100 ms, so
groups can contain fewer than 32 messages.

Each short control offered 6,000 writes over 30 seconds. These are single samples,
useful for causal direction rather than a precise production capacity estimate.

| Receiver control | Replicas applied at offer end | End-to-end writes/s, including catch-up | Slower replica P95 age | Drain |
| --- | --- | --- | --- | --- |
| Standard, eight / serial ACKs | 1,662 / 1,616 | 117.13 | 31.82 s | 21.19 s |
| Eight / concurrent confirmed ACKs | 2,704 / 2,720 | 121.63 | 23.86 s | 19.32 s |
| Up to 32 / serial ACKs | 3,143 / 3,136 | 129.74 | 20.17 s | 16.21 s |
| Up to 32 / concurrent confirmed ACKs | 3,511 / 3,483 | 128.59 | 16.29 s | 16.65 s |

Overlapping confirmations improves replica progress with the same committed
mutations and ACK guarantees. Larger groups also amortize fetch, validation and
commit costs. Neither experiment sustains the offered rate. Concurrent ACK timing
sums overlap and cannot be interpreted as a fraction of wall time; cycle duration
and applied progress are the relevant measures for those controls.

The unprofiled two-minute comparison offered 24,000 writes per case. The same
standard and combined controls each ran once; no confidence interval is claimed.

| Two-minute measurement | Standard receiver | Up to 32 / concurrent confirmed ACKs |
| --- | --- | --- |
| Owner local commit rate | 199.97/s | 199.88/s |
| Replica rate during seconds 30–110 | 59.67–59.71/s | 112.58–112.73/s |
| Replica counts at ingestion end | 7,088 / 7,088 | 13,371 / 13,369 |
| Owner publication checkpoint at ingestion end | 14,241 | 12,891 |
| End-to-end rate including catch-up | 114.45/s | 123.01/s |
| Slower replica P95 age | 124.97 s | 72.21 s |
| Post-offer drain | 89.67 s | 75.04 s |
| Peak outbox count | 9,759 | 11,109 |
| Peak Go heap | 75.80 MiB | 48.46 MiB |
| Peak process RSS | 116.52 MiB | 87.73 MiB |

Replicas now track broker publication much more closely. End-to-end throughput
improves about 7.5%, and replica P95 age falls about 42%, but publication checkpoint
progress is lower and outbox growth is worse. This is an ACK/lag improvement,
not a demonstrated 200/s fix or an across-the-board performance improvement.
Replica counts can exceed the owner's saved publication checkpoint because
replicas can consume accepted messages before the bounded owner's group checkpoint.
Checkpoint counts therefore do not represent the broker's exact accepted count.

All eight full-workload cases, including the diagnostic profile cases, completed
with 120,000 committed writes published and applied on both replicas, matching
all records and logical ledgers. Final outboxes were empty, and every offered
message had a confirmed publisher ACK and both explicit replica ACKs. The isolated
broker cases separately verified another 12,000 retained messages and their order.

## Durable component limits

Two isolated broker runs use the same authenticated R1 file stream, sync-always
policy, 1 KiB values encoded as real changes, expected sequences, stable IDs,
disabled retries, and eight sends awaiting ordered ACK validation. Preparation
and all retained-byte/order verification occur outside the timed interval. No
owner writes, replica traffic, history reads or database checkpoints run during
publication. This isolates a component, not the complete jlite contract.

| Component | Sample 1 | Sample 2 |
| --- | --- | --- |
| Broker, 6,000 confirmed publications | 200.39/s | 200.92/s |
| Broker ACK latency P95, overlapping sends | 55.90 ms | 54.76 ms |
| File append + fsync, 1,000 operations | 213.04/s | 213.22/s |
| File append + fsync P50 | 4.73 ms | 4.64 ms |

The file test appends 1,640 bytes and calls `File.Sync` once per operation on the
same data volume. It does not emulate broker metadata or concurrent database
traffic. Together with broker source and profiles, these measurements identify
disk synchronization as a concrete limit, with almost no headroom at 200/s before
the additional work. They are not a universal limit for NATS, SQLite or jlite.

For 200/s, receiver ACK confirmation should be overlapped with bounded concurrency
after the atomic commit. The broker also needs more durable-write headroom: faster
or physically isolated storage, or broker-level durable group commit that still
confirms every publication only after persistence. jlite's existing publication
and checkpoint batching does not batch the broker's per-message fsync. Broker
group commit would require explicit protocol/policy and recovery work; it is not
implemented or benchmarked here. Separate containers or named volumes on the
same disk do not establish independent I/O capacity. Increasing the async window,
removing history validation or weakening sync settings is not a demonstrated fix.

## Memory

The earlier matched benchmark recorded peak Go heap increasing from about
46 to 90 MiB, and process RSS from 85 to 130 MiB. Those are process peaks, not
retained heap or a per-node memory bound: the benchmark embeds the broker, owner,
two replicas and measurement bookkeeping in one Go process.

| Snapshot, MiB | Window barriers | Continuous refill |
| --- | --- | --- |
| Heap at ingestion end, before → after GC | 32.93 → 18.75 | 35.78 → 25.73 |
| Heap after drain, before → after GC | 34.78 → 22.75 | 32.17 → 16.63 |
| Total bytes allocated through drain | 4.18 GiB | 4.76 GiB |
| Goroutines at ingestion end | 65 | 65 |
| Goroutines after drain | 55 | 54–55 |

The sampled ingestion-end heap attributes 16 MiB in the continuous case to two
8 MiB NATS message-block pool buffers, versus one 8 MiB buffer in the windowed
case. This accounts for most of the retained-heap difference at that snapshot.
The publisher's bounded future ring is not the dominant retained allocation.

The continuous allocation profile samples about 4.72 GiB through completion:
`bytes.Clone` contributes 1.23 GiB, JSON decoder buffering 0.82 GiB, and broker
large-block pool allocations 0.26 GiB. Clone callers include repeated change
normalization, identity validation and JSON encoding. Those cumulative call paths
overlap; their totals must not be added together. This is allocation churn, not
4.72 GiB simultaneously resident in memory.

GC reduces live heap without immediately returning all pages to the OS. At the
continuous drained snapshot, live heap is 16.63 MiB while RSS is about 87 MiB.
The samples show broker buffers, transient allocations and allocator high-water
behavior; they do not show an accumulating goroutine leak. Two snapshots cannot
attribute every byte of the earlier peak or rule out every possible leak. Reducing
copies is a separate memory optimization, not the measured ACK bottleneck.
The continuous profiled run records only 33.95 ms of total GC pauses through
roughly 202 seconds, with reported GC CPU fraction about 0.19%. Neither metric
explains the observed throughput shortfall.

## Method and evidence

The configured Podman remote is Fedora 44, with btrfs named-volume storage.
Measurement runs use four CPUs, 2 GiB, GOMAXPROCS=4, Go 1.27.1, Zova 1.1.0,
NATS server 2.15.0 and NATS Go 1.54.0. The broker and all databases share that
volume. Runs use fresh per-case storage and execute sequentially. No build,
test or profile-analysis jobs overlap the unprofiled control/component timings.
The physical host remains shared with unrelated work. One brief pprof analysis
overlapped the profiled windowed run; its timings are not used for capacity claims.

Evidence: [raw replica cases](results/replication-diagnostics.json),
[component cases](results/replication-components.json),
[environment and binary hashes](results/replication-diagnostics-environment.json),
[replica source checksums](results/replication-diagnostics-source.sha256),
[component source checksums](results/replication-components-source.sha256), and
[profiles](results/replication-diagnostics-profiles/).

The original full race suite passed after adding replica observers (jlite 112.825s,
command package 1.867s), and vet passed. The subsequently added component helpers
passed focused race checks (5.991s). The replica diagnostic unit test observes
fetch/commit-before-ACK ordering; real-storage integration tests exercise all four
receiver modes and verify converged records and logical ledgers.

Relevant upstream source:
[sync-always stream fsync](https://github.com/nats-io/nats-server/blob/v2.15.0/server/filestore.go#L8777),
[consumer ACK handling](https://github.com/nats-io/nats-server/blob/v2.15.0/server/consumer.go#L3128),
and [large-block buffer pool](https://github.com/nats-io/nats-server/blob/v2.15.0/server/filestore.go#L1071).

## Reproduction

Use the configured Podman remote directly. The build stage pins Go 1.27.1 and
runs race checks and vet. Following the existing benchmark container setup:

```sh
podman build --target build -f deploy/podman/Containerfile -t localhost/jlite:replica-diagnostics .
podman volume create jlite-replica-diagnostics-data
podman run -d --name jlite-replica-diagnostics --network=none --cpus=4 --memory=2g \
  --pids-limit=512 --volume jlite-replica-diagnostics-data:/bench-data \
  --env TMPDIR=/bench-data --env GOMAXPROCS=4 localhost/jlite:replica-diagnostics sleep infinity
podman exec jlite-replica-diagnostics sh scripts/go.sh test -c -o /tmp/diagnostics.test
podman exec \
  -e 'JLITE_CONTINUOUS_CASE={"Rate":200,"Operations":24000,"PayloadBytes":1024,"Round":1,"PublicationMode":"batch","DiagnosticMode":"profile"}' \
  -e JLITE_CONTINUOUS_CASE_OUTPUT=/bench-data/standard.json \
  jlite-replica-diagnostics /tmp/diagnostics.test -test.run='^TestContinuousBenchmarkCase$' -test.v -test.timeout=9m
```

Run cases sequentially, changing `DiagnosticMode` and output filename for each
control: `profile` observes normal serial ACK behavior; `parallel-acks`, `batch32`
and `batch32-parallel-acks` select the interventions. Despite its name, `profile`
does not activate runtime profiling unless `JLITE_DIAGNOSTIC_DIR` is set. Set that
variable to a unique directory and add `-test.cpuprofile=/bench-data/case.cpu` for
profile collection. Keep those cases separate from capacity measurements.

Component commands on the same idle container:

```sh
podman exec -e JLITE_BROKER_DIAGNOSTIC_OPERATIONS=6000 \
  -e JLITE_BROKER_DIAGNOSTIC_OUTPUT=/bench-data/broker.json \
  jlite-replica-diagnostics /tmp/diagnostics.test -test.run='^TestDurableBrokerDiagnostic$' -test.v -test.timeout=5m
podman exec -e JLITE_STORAGE_DIAGNOSTIC_OUTPUT=/bench-data/storage.json \
  jlite-replica-diagnostics /tmp/diagnostics.test -test.run='^TestDurableStorageDiagnostic$' -test.v -test.timeout=5m
```

Copy JSON/profile artifacts with `podman cp` before removing this container and
volume. Inspect heap and allocation attribution with `go tool pprof -top`;
use `-sample_index=alloc_space` for allocation profiles. Reconstruct the old
publisher in an isolated source copy using the previously saved
[production patch](results/continuous-pipeline.patch), with the same observer
harness, when comparing stages against the window-barrier implementation.
