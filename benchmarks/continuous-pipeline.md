# Bounded continuous publication

**Status: not adopted.** The publisher was returned to window barriers on
2026-10-13. This report is kept as the record of the experiment; "now" and
"current" below describe the experimental code, saved as
[a patch](results/continuous-pipeline.patch).

Continuous refill preserved correctness but did not sustain 200 replicated
writes/s in the tested setup. Short-offer throughput improved 115 → 129 writes/s;
two-minute throughput was effectively unchanged at 111 → 112 writes/s, with
worse replica lag and higher process memory. This change is not a demonstrated
200/s performance fix.

The subsequent [replica and storage investigation](replication-diagnostics.md)
measures the receiver ACK bottleneck, explains retained memory and transient
allocation growth, and tests confirmed ACK concurrency without weakening durability.

The publisher now replenishes its ACK window after validating each oldest ACK,
instead of waiting for all eight ACKs and retained reads before sending another
window. The default cap remains eight sends awaiting ACK validation. The complete
uncheckpointed group remains bounded by 500 operations and 1 MiB of encoded data.

After the group's ACKs drain, every retained subject, change ID and payload is
verified before one atomic outbox/checkpoint transaction. Stable message IDs,
expected physical sequences, disabled SDK retries, and recovery after expired
broker deduplication remain unchanged. A failure returns zero checkpointed writes,
retains the whole uncheckpointed group and closes an uncertain publishing connection.
Replicas can apply messages before the owner's group checkpoint commits.

## ACK measurements and offered load

`AckWait.Calls` counts observed asynchronous responses. `AckWait.TotalMS` sums
elapsed time from entering `PublishAsync` to observation of its ACK/error;
`AckLatency` reports the corresponding P50/P95/P99. ACKs are observed independently
of when the publisher reaches their futures. These durations overlap and include
enqueue, broker processing, transport and observer scheduling; their sum is not
publisher wall time or a measurement of disk synchronization alone. The synchronous
`Publish` timing continues to cover the single-entry reference path.

The observer is confined to the benchmark harness. Production publication adds
no goroutines or instrumentation API. Pending response work is bounded by the
publication window, and all observers are joined during cancellation/cleanup.

Arrivals use absolute scheduled deadlines. A delayed load generator catches up
rather than dropping timer ticks. `OfferedOpsPerSecond` records its actual offer,
and `OfferToCommit` includes delay from scheduled arrival through local commit,
including the bounded driver queue. Existing `LocalLatency` measures the Write call
itself. Neither local commit nor broker ACK implies application on both replicas.

## Method

The paired comparison freezes the previous window-barrier publisher and compiles
it with the updated measurement harness, then compares it with the continuous
publisher. Both binaries use Go 1.27.1, Zova 1.1.0, NATS server 2.15.0, SQLite
WAL/FULL, broker file storage with `sync_interval: always`, 1 KiB values, 32 write
workers, and both replica consumers running throughout ingestion and catch-up.

The short comparison uses three repetitions at 20, 80 and 200 writes/s for eight
seconds per offer. The sustained comparison uses two repetitions at 200 writes/s
for 120 seconds per offer (24,000 writes each). Variant order reverses in round
two. Each child uses fresh databases and broker storage on a dedicated named
Podman volume. Cases run sequentially in a four-CPU, 2 GiB, network-disabled
container with GOMAXPROCS=4 on the configured Fedora remote. Physical host storage
and CPUs remain shared with other work.

`EndToEndOpsPerSecond` includes post-offer catch-up. Publication ages and outbox
counts describe saved group checkpoints, which can lag broker acceptance and
replica application by a bounded group. Replica progress and backlog growth during
the offer determine whether the requested arrival rate is sustained; eventual
convergence alone does not.

## Reproduction

The frozen baseline can be reconstructed by reversing the saved production patch
in an isolated container and compiling its test binary with
`sh scripts/go.sh test -c`. Run only the benchmark case on that binary; the new refill regression test
intentionally fails against the old implementation. The final pipeline's image
build runs the full correctness suite before measurement.

The ordinary comparison remains:

```sh
sh scripts/podman-continuous-benchmark.sh
```

For the current publisher's two-minute 200/s workload:

```sh
JLITE_BENCH_SECONDS=120 JLITE_BENCH_RATES=200 JLITE_BENCH_MODES=batch \
  JLITE_BENCH_ROUNDS=2 sh scripts/podman-continuous-benchmark.sh \
  benchmarks/results/sustained-current.json
```

The paired baseline was compiled before modifying `publisher_batch.go`, with the
same updated case driver. [The production patch](results/continuous-pipeline.patch)
records the exact publisher difference; reversing it in an isolated source copy
reconstructs that baseline. The raw report records the baseline publisher and test
binary hashes. [Measured source checksums](results/continuous-pipeline-source.sha256)
identify the final source and harness. No timing jobs overlapped build or test jobs.

## Verification

The final image build ran the complete race suite, vet and binary build successfully
before measurements (jlite race suite 43.972s, command suite 1.106s). A causal test
holds the second real broker ACK until the ninth send: continuous refill advances,
whereas a window barrier stalls. It also checks the ACK window and deferral of
retained reads until the group's ACKs drain.

Further tests cover failure on the tenth send, retained-read timeout, cancellation,
and invalid read-back after refill. They preserve the entire outbox, reopen after
the deduplication window expires, and converge on both replicas without duplicate
logical effects. Existing tests cover count/byte bounds, one checkpoint transaction,
partial sends, corrupted middle history, and SIGKILL before and after checkpointing.

## Results

All 22 final cases completed with 110,400 committed writes published and applied
by both replicas with matching records and logical ledgers. Every final outbox was
empty; all ACK counts matched their offered operation counts, the pending ACK cap
remained eight, and checkpoint groups stayed within configured limits. Workload
test success establishes convergence and correctness, not that 200/s was sustained.

The short results below are medians of three samples. Replica P95 uses the slower
replica's P95 per sample; end-to-end throughput includes post-offer catch-up.

| Offered writes/s | End-to-end writes/s, windowed → continuous | ACK P95 | Replica P95 | Post-offer drain |
| --- | --- | --- | --- | --- |
| 20 | 19.68 → 19.56 | 9.61 → 9.48 ms | 123.70 → 124.87 ms | 103 → 162 ms |
| 80 | 78.16 → 78.14 | 37.29 → 55.56 ms | 157.12 → 228.67 ms | 172 → 162 ms |
| 200 | 114.91 → 129.06 | 113.67 → 83.59 ms | 5.50 → 6.81 s | 5.91 → 4.38 s |

At the short 200/s offer, end-to-end throughput improved 12.3%; the complete
ranges were 108.34–117.57 → 128.75–132.17 writes/s. Local Write P95 improved
70.57 → 47.09ms, retained-read time fell 1,020 → 642ms, and peak outbox payload
fell 1.45 → 1.03MiB. However, replica P95 increased 24%. At 80/s, throughput was
effectively unchanged while local Write P95 increased 34.46 → 47.09ms and replica
P95 increased. These adjacent latency regressions matter despite faster final drain.

The two-minute results are medians of two samples per variant, each offering
24,000 writes at an observed 200.00/s. Both owners committed approximately 200/s.

| Metric at the 120-second 200/s offer | Window barriers | Continuous ACK refill |
| --- | --- | --- |
| End-to-end writes/s, including catch-up | 110.99 (109.20–112.79) | 112.23 (111.47–113.00) |
| Post-offer drain | 96.25 s | 93.83 s |
| Slower replica P95 age | 92.05 s | 130.42 s |
| ACK P95 | 112.51 ms | 113.95 ms |
| Local Write P95 | 81.65 ms | 85.11 ms |
| Scheduled arrival to local commit P95 | 84.51 ms | 88.49 ms |
| Peak outbox payload | 17.79 MiB | 16.26 MiB |
| Process RSS high-water | 84.90 MiB | 129.62 MiB |
| Go heap peak | 45.66 MiB | 89.86 MiB |

The longer workload shows only a 1.1% end-to-end difference and substantially
worse replica lag and process memory. This does not demonstrate a useful increase
in sustained replicated capacity. RSS includes the embedded broker, three database
handles and benchmark bookkeeping; Go heap excludes native allocations. Neither
measurement is a memory ceiling for a deployed node.

During seconds 30–110 of the sustained offers, the baseline replicas progressed
at approximately 104–107 writes/s; continuous-pipeline replicas progressed at
approximately 53 writes/s. At ingestion completion the baseline replicas had
applied 12,357–12,873 of 24,000 writes; continuous replicas had applied only
6,314–6,594. Publication checkpoints advanced further with continuous refill, but
replica backlog grew faster. Group-checkpoint delay cannot explain that replica
backlog: these are their committed applied positions.

**Neither variant sustained 200 replicated writes/s in this setup.** The faster
publisher occupies more of the shared storage/processing path while writes arrive;
the observed contention and ACK costs still limit replication. ACK observations
do not identify how much time belongs to disk sync, scheduling or individual
consumer acknowledgement processing. The pipeline preserves correctness but is
not a demonstrated fix for the 200/s operating target.

The earlier [checkpoint-batching benchmark](publication-batching.md) measured
117.47 writes/s at its short 200/s offer. The current pipeline measured 129.06;
the fresh paired baseline (114.91) is the stronger comparison because both variants
used the same updated arrival scheduler and ACK instrumentation. The historical
run used timer ticks that could be dropped under scheduling delay. Neither short
benchmark established steady-state capacity.

Evidence:

- [All 22 final samples](results/continuous-pipeline.json)
- [Remote environment, resource limits and binary identities](results/continuous-pipeline-environment.json)
- [Window-barrier CPU profile](results/continuous-pipeline.json.windowed.cpu)
- [Continuous-pipeline CPU profile](results/continuous-pipeline.json.pipeline.cpu)
- [Window-barrier blocking profile](results/continuous-pipeline.json.windowed.block)
- [Continuous-pipeline blocking profile](results/continuous-pipeline.json.pipeline.block)

The first sustained pipeline child printed `PASS` and saved its completed result,
but Podman returned exit 125 because its exec-session record was unavailable.
The result was recovered, matched against the recorded 110-second progress and
verified for all 24,000 commits, ACKs, publications, replica effects and empty
outbox. The transport issue is recorded in the raw report; it was not treated as
a jlite failure or silently discarded. Remaining cases used unique output paths.

## Initial candidate

The first implementation refilled only after reading each acknowledged message
back. In its first 200/s pair, retained-read time increased from 567ms to 9,250ms
and end-to-end throughput fell from 151.79 to 134.51 writes/s despite lower ACK
latency. The reads overlapped broker writes; the final implementation separates
those phases while retaining all validation before checkpointing.

The [12 completed initial samples](results/continuous-pipeline-initial.json),
[initial source manifest](results/continuous-pipeline-initial-source.sha256), and
[initial production patch](results/continuous-pipeline-initial.patch) are retained
separately. That experiment stopped before its sustained cases and is excluded
from the final comparison.
