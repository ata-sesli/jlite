# Concurrent replica ACK confirmation

**Status: not adopted.** Replica ACKs were returned to serial confirmation on
2026-10-13, together with the continuous publisher this change was measured on.
This report is kept as the record of the experiment; "now" below describes the
experimental code, saved as [a patch](results/replica-acks-production.patch).

Paired sustained runs show lower replica lag and memory peaks, with effectively
unchanged end-to-end capacity. The 200 writes/s target remains unmet; short 200/s
offers also show a small throughput regression.

The receiver now confirms explicit broker ACKs with at most eight workers after
its atomic replica transaction commits. Fetch count, byte limits, wait time,
broker policy and database durability remain unchanged. Mutation application
stays ordered and atomic; only confirmation requests overlap.

All workers share the existing batch `Connection.RequestTimeout` context and
are joined before `ConsumeNext` returns. An ACK failure preserves the applied
count and durable checkpoint, waits for the other workers, and returns joined
errors. Already committed deliveries can be confirmed despite another ACK's
failure. Redelivery still checks the durable ledger; reconnection still resumes
from the local physical checkpoint rather than the broker's ACK floor. No AckAll
policy, early ACK, public option or inline retry loop is introduced.

## Correctness checks

A causal real-broker regression holds all eight confirmations at their transport
boundary. The previous receiver starts only the first ACK and fails the check;
the new receiver starts all eight after the complete database checkpoint and
ledger are committed. It returns only after every real DoubleAck completes.

Additional checks cover:

- One failed ACK before the broker receives it, followed by real broker
  redelivery that applies zero new logical changes.
- A broker-confirmed ACK whose success is hidden from the caller, followed by
  reopen and ordered catch-up with the same durable ledger.
- A failed confirmation while other workers remain blocked: the receiver waits
  for those workers before returning the positive applied count and error.
- Cancellation and timeout after commit, with no ACK worker surviving return.
- A larger synthetic transport group, proving the independent eight-worker cap.
- SIGKILL in the actual ConsumeNext path after commit, with the first ACK held
  while seven later deliveries have real broker confirmations. The broker floor
  remains zero with one pending delivery. Reopen retains checkpoint eight;
  subsequent catch-up matches the owner and a fresh replica without duplicates.
- Existing rollback, malformed history, namespace isolation, offline replay,
  publication recovery and shutdown checks in the full race suite.

Podman remote verification used Go 1.27.1 with `GOTOOLCHAIN=local`. Focused race
checks passed in 17.978s; the full race suite passed in 138.366s for jlite and
1.787s for the command package. Vet and the CLI build passed. These are process
crash/intact-disk checks, not power-loss or distributed-failover tests.

## Comparison method

Both binaries use the same final benchmark harness and regression-test sources.
The serial variant restores the previous ConsumeNext ACK loop. The new helper
remains compiled for the worker-bound test but is not called by the serial
receiver. Only the loop and its comment differ between comparison variants.
The complete production change is saved separately from that comparison patch.

Short offers run at 20, 80 and 200 writes/s for eight seconds, two samples per
variant/rate. Sustained offers run at 200 writes/s for 120 seconds, two samples
per variant. Each case uses 1 KiB values, 32 write workers, continuous publication
and two active replicas throughout ingestion and catch-up. Round one runs serial
first; round two reverses the order. No runtime CPU/heap profiling or forced GC
is enabled during these comparisons. The diagnostic `profile` mode adds stage
timers only when `JLITE_DIAGNOSTIC_DIR` is unset.

The configured macOS Podman remote is Fedora 44. Cases execute sequentially with
fresh storage on a dedicated btrfs named volume in a four-CPU, 2 GiB container,
GOMAXPROCS=4 and no external network. Broker and three database handles share one
process and volume. No build, test or profile-analysis jobs overlap timing; the
physical host is shared with existing unrelated containers.

Both variants retain Zova 1.1.0 WAL/FULL, NATS server 2.15.0 R1 file storage with
`sync_interval: always`, NATS Go 1.54.0, default fetch count eight, 100 ms fetch
wait, 2 MiB fetch/header budget and MaxAckPending eight. Publication remains the
existing continuous pipeline: eight sends awaiting ordered ACK validation,
500-operation / 1 MiB checkpoint groups, retained-history verification, stable
IDs, expected physical sequences and disabled SDK retries.

End-to-end rate includes catch-up; steady replica progress and backlog growth
determine whether an arrival rate is sustained. Saved publication checkpoints
can trail broker acceptance by a bounded group. ACK timing sums overlap in the
new receiver and cannot be interpreted as a percentage of cycle wall time.
Process RSS includes broker, three databases and benchmark bookkeeping; it is
not a per-node memory limit. Two samples provide a comparison, not a production
capacity guarantee or confidence interval.

Evidence: [raw paired results](results/replica-acks.json),
[environment and binary hashes](results/replica-acks-environment.json),
[serial source hashes](results/replica-acks-serial-source.sha256),
[concurrent source hashes](results/replica-acks-concurrent-source.sha256),
[production patch](results/replica-acks-production.patch), and
[comparison patch](results/replica-acks-benchmark.patch).

## Short-offer results

The table reports medians of two samples per variant. Replica P95 uses the slower replica in each case.

| Offered writes/s | End-to-end writes/s, serial → concurrent | Replica P95 age | Publication P95 age | Local Write P95 |
| --- | --- | --- | --- | --- |
| 20 | 19.68 → 19.73 | 124.45 → 124.71 ms | 17.84 → 20.01 ms | 20.73 → 22.20 ms |
| 80 | 78.16 → 78.74 | 196.60 → 181.84 ms | 134.72 → 124.08 ms | 39.79 → 38.30 ms |
| 200 | 118.61 → 114.32 | 8156.19 → 6919.42 ms | 6334.45 → 7014.78 ms | 72.86 → 65.83 ms |

At the short 200/s offer, replica P95 age improves about 15%, while end-to-end throughput falls about 3.6% and publication P95 age rises about 11%. The complete end-to-end ranges are 118.37–118.85/s serial and 113.50–115.15/s concurrent. Low-rate throughput is close; two samples and differing latency ranges do not establish a general low-rate latency improvement.

## Sustained results

These are medians of two 120-second cases per variant, each offering 24,000 writes at 200/s. Steady replica rate uses the slower replica in each case.

| Metric | Serial ACKs | Concurrent ACKs, same fetch size |
| --- | --- | --- |
| Local commit rate | 199.94/s | 199.89/s |
| Steady slower-replica rate, seconds 30–110 | 52.74/s | 82.59/s |
| End-to-end rate, including catch-up | 117.92/s | 116.89/s |
| Slower replica P95 age | 128.76 s | 101.37 s |
| Post-offer drain | 83.52 s | 85.28 s |
| Peak process RSS | 124.28 MiB | 103.75 MiB |
| Peak Go heap | 79.19 MiB | 64.34 MiB |
| Peak outbox count | 10428.00 writes | 10442.50 writes |
| Local Write P95 | 85.21 ms | 81.83 ms |
| Publication ACK P95 | 110.64 ms | 110.84 ms |

Steady replica progress improves about 57%, slower-replica P95 age falls about 21%,
and process RSS peak falls about 17% in this workload. Sustained end-to-end rate
is effectively unchanged (117.92 → 116.89/s), with a roughly 1.8-second longer
median drain. The full rate ranges overlap: 116.93–118.90/s serial and
115.75–118.03/s concurrent. Peak outbox counts are essentially unchanged.
The owner commits about 200/s, but publication/replication cannot sustain that
arrival rate. This implements a replica lag improvement, not a 200/s capacity fix.

The earlier continuous-pipeline report measured 111.47–113.00/s with serial ACKs;
the fresh serial control is faster at 116.93–118.90/s. Comparing only with that
older number would incorrectly attribute host/run variation to this change.
The paired control is the evidence for the current implementation. The prior
batch32 + parallel-ACK experiment is a different receiver configuration and is
not the baseline for this change.

All 16 cases verified 105,600 committed writes published and applied on both
replicas, exact records and logical ledgers, every publisher/replica ACK and
empty final outboxes. Warm-up writes are excluded from those operation counts.
No application failures or Podman transport recovery occurred. One reporting-only
runner error occurred after the first passing result had been saved; that result
was retained and the remaining cases resumed. An isolated baseline-generation
compile error was corrected before any measurement.

## Reproduction

Use the configured Podman remote directly. Build the current test stage with
Go 1.27.1; compile the concurrent test binary with `sh scripts/go.sh test -c`.
Inside an isolated container/source copy, reverse
`results/replica-acks-benchmark.patch` to restore the serial loop and compile
the serial binary with the identical harness. Reapply the patch before ordinary
correctness checks: the concurrency regression deliberately fails on the serial
receiver. `results/replica-acks-production.patch` records the entire production
change, including the helper, and is separate from the comparison patch.

Follow the existing benchmark's four-CPU, 2 GiB, GOMAXPROCS=4 container setup
with `--network=none`, a dedicated named volume mounted at `/bench-data`, and
`TMPDIR=/bench-data`. Run the binaries sequentially with unique output paths:

```sh
podman exec \
  -e 'JLITE_CONTINUOUS_CASE={"Rate":200,"Operations":24000,"PayloadBytes":1024,"Round":1,"PublicationMode":"batch","DiagnosticMode":"profile"}' \
  -e JLITE_CONTINUOUS_CASE_OUTPUT=/bench-data/concurrent-r1.json \
  jlite-ack-run /tmp/concurrent-acks.test -test.run='^TestContinuousBenchmarkCase$' -test.v -test.timeout=9m
```

Switch the binary and output filename for serial cases; set Round to two and
reverse variant order for the second pair. Short cases use Operations equal
to eight times Rate. Leave `JLITE_DIAGNOSTIC_DIR` unset and omit CPU profiling
flags for this comparison. Copy JSON results with `podman cp` before removing
the test container and its volume. The existing ordinary benchmark script
remains available for the current receiver; its aggregate profiling workflow
is distinct from these unprofiled paired case runs.
