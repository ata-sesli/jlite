# Batching measurements

The October 8, 2026 Fedora run completed 42 isolated samples: three repetitions
of six payload/arrival combinations in each transaction mode, plus six overload
samples. All committed writes were published and applied by both replicas, and
every sample checked record contents, ledger counts and final progress.

On this workload, batching improved local burst throughput and offline replay.
It did not deliver a comparable improvement in publication drain time. At low
arrival rates, throughput stayed near the imposed rate; latency generally s
increased with the collection window. These measurements concern the keyed-put
prototype and its explicit writer/publisher/consumer APIs.

## Reproduce

From the repository directory, with Podman connected to a Linux/amd64 engine:

```sh
sh scripts/podman-benchmark.sh
# Optional output path or repetition count:
JLITE_BENCH_ROUNDS=3 sh scripts/podman-benchmark.sh benchmarks/results/repeated.json
```

The script first runs the full race suite and vet in the build stage using Go
1.27.1. Measurements then use non-race test binaries. It creates a separate
container and data volume, publishes no ports, copies results back, and removes
its container/volume. It does not operate on the demo's data. A failed measured
run copies available partial results with a `.partial` suffix.

Evidence for this run:

- [Raw per-sample JSON](results/fedora.json)
- [Fedora/Podman environment](results/environment.json)
- [Source checksums](results/source.sha256)
- [Harness](../operating_benchmark_test.go) and [correctness checks](../benchmark_checks_test.go)

The source revision was `a3517f54512d5c33a2ece599f25c99898100bb09` with the
benchmark changes uncommitted. The checksum file identifies those measured
sources. The run took 665 seconds, excluding image verification/build.

## Workload and controls

The host was Fedora 44, Linux/amd64, Intel Core i7-6700HQ at 2.60 GHz with eight
logical CPUs and about 11.6 GiB RAM. Podman engine 5.8.4/client 6.1.0 used btrfs
backing storage. The measured container had a four-CPU quota, `GOMAXPROCS=4`,
2 GiB memory limit and a dedicated named data volume. This was a shared host;
CPU quota and a separate volume do not isolate physical CPU/storage contention.

Both modes used identical Zova 1.1.0 databases, record/request/outbox indexes,
payloads, authenticated NATS 2.15.0 and one retained file stream. SQLite used
WAL/FULL; JetStream used `sync_interval: always`. Zova/native setup and the Go
toolchain were pinned by the existing Containerfile.

Single mode limited write and replica batches to one operation. Batch mode used
500 operations, 1 MiB encoded bytes and 10 ms collection time. Replica fetches
also obeyed conservative count/byte limits and a 100 ms maximum fetch wait;
effective batch-mode fetch count was eight with the default wire ceiling.

Low-rate cases offered 128 operations at 100/second with 32 callers. Burst cases
offered 1,024 operations through 512 concurrent callers. Ordinary cases kept the
1,024-operation admission limit but used **16 MiB** admitted bytes in both modes,
so large-value bursts fit without rejection; this differs from the 4 MiB default.
Outbox and retained stream budgets stayed at 64 MiB. Values were synthetic binary
puts filled with `0x5a`, using distinct measured keys/request IDs; results are not
a varied-content, mixed-operation or rich-state benchmark.

Each child process created fresh storage, warmed eight records through both
replicas, and installed an identical SQL counter trigger in both modes. That
trigger records actual committed operations/encoded bytes per transaction and
adds instrumentation overhead. A 10 ms sampler records admission/backlog and
Go heap usage. Alternate repetitions reversed the transaction-mode order. No
sample or outlier was discarded.

Measurements deliberately have three phases:

1. Commit local writes with publication paused, isolating the coordinated writer.
2. Drain publication with one replica consuming concurrently.
3. Reconnect the second replica and time its offline catch-up from the warm prefix.

The driver calls the lower-level APIs directly, without the demo's control-socket
overhead or Node's idle/retry loop. Lag includes the deliberately accumulated
backlog, so it is not an application's continuously running replication SLA.

## Local burst results

Each number is the median of three runs. Ranges show minimum–maximum local
throughput. P95 is the median of the three per-run nearest-rank P95s, rather
than a percentile pooled across runs.

| Value | Single ops/s (range) | Batch ops/s (range) | Median ratio | Local P95, single → batch | Mean operations/transaction, batch |
| --- | --- | --- | --- | --- | --- |
| 64 B | 159 (149–234) | 2,428 (2,372–7,824) | 15.3× | 3,340 → 282 ms | 256 |
| 1 KiB | 145 (137–148) | 1,912 (1,807–3,657) | 13.2× | 3,787 → 282 ms | 256 |
| 16 KiB | 112 (109–178) | 555 (367–611) | 5.0× | 4,664 → 1,138 ms | 44.5 |

Ratios divide mode medians. Large 512-caller queues explain the multi-second
single-mode tail latency; these are saturation samples. The 16 KiB batch was
byte-limited: observed maximum 47 operations, while smaller payloads reached the
500-operation ceiling. Throughput variance was substantial, particularly for
64-byte batches, so the ratios should not be treated as universal speedups.

## Low-rate results

Both modes achieved approximately 98–100 operations/second against the
100/second offer. Collection produced much smaller batches.

| Value | Local P95, single → batch | Mean batch operations |
| --- | --- | --- |
| 64 B | 4.82 → 20.18 ms | 1.6 |
| 1 KiB | 9.06 → 24.95 ms | 1.9 |
| 16 KiB | 46.58 → 39.42 ms | 2.4 |

The 16 KiB low-rate results did not show the same latency direction as smaller
values. Across all cases, batching changed transaction grouping and queueing;
it did not increase the imposed low arrival rate.

## Publication and recovery

These are medians for draining the 1,024-write burst backlog. Publication/replica
ages are measured from each successful local response to observed publication
progress or replica-consumer return, which includes its acknowledgements.

| Value | Drain with online replica, single → batch | Offline catch-up, single → batch | Publication-age P95, single → batch | Online replica-age P95, single → batch |
| --- | --- | --- | --- | --- |
| 64 B | 18.39 → 15.37 s | 9.82 → 1.77 s | 17.85 → 14.54 s | 17.85 → 14.67 s |
| 1 KiB | 18.34 → 16.17 s | 9.46 → 1.80 s | 17.87 → 15.18 s | 17.87 → 15.20 s |
| 16 KiB | 26.21 → 21.54 s | 12.01 → 2.35 s | 25.36 → 20.47 s | 25.36 → 20.57 s |

Drain throughput was about 56–67 writes/second for small values and 39–48 for
16 KiB values. The publisher still publishes and checkpoints one ordered change
at a time with retained-history verification. Local transaction batching does
not remove that serial publication path. Offline replica batching improved
catch-up by roughly 5.1–5.5× in the median comparisons.

## Overload, memory and storage

The overload workload offered requests from 128 callers for 1.5 seconds while
publication stayed paused. Both modes used 64 admitted operations, 1 MiB admitted
bytes and 1 MiB outbox payloads with 16 KiB values. Time includes completion of
requests already in flight at the end of the offer window.

Each of all six runs committed **47** writes before filling the outbox. Other
attempts explicitly returned admission or outbox-full errors. The byte ceiling
bound first: maximum sampled pending count was 47, pending bytes stayed below
1,038,700 and outbox bytes below 1,037,820. Every committed write later published
and reached both replicas.

| Mode | Attempts across runs | Process baseline RSS | Process peak RSS across runs | Sampled peak Go heap |
| --- | --- | --- | --- | --- |
| Single | 1,134–2,975 | about 31.2–31.6 MiB | 42.1–42.9 MiB | 12.6–13.3 MiB |
| Batch | 2,266–2,564 | about 31.3–31.6 MiB | 43.2–45.9 MiB | 13.1–14.4 MiB |

RSS is Linux `VmHWM` for each isolated child, including warm-up and all measured
phases. Go heap is sampled `HeapAlloc`, and excludes native SQLite allocations.
Both include the embedded broker, three databases and measurement bookkeeping.
Queue samples can miss transient peaks; capacity invariants and end-state checks
also run in the harness. A payload-byte limit is not a process-RSS ceiling.

For the ordinary 16 KiB burst, median process high-water RSS was 102.9 MiB in
single mode and 110.6 MiB in batch mode. At the storage snapshot after publication,
owner main files were about 39 MiB with 4–5.8 MiB WAL files, and retained stream
bytes were 21.9 MiB. SQLite retains freed outbox pages; file size cannot be
attributed solely to request metadata or current live values.

There were 1,032 logical writes including warm-up, requiring 1,032 owner retry
rows and 1,032 change-ledger rows **per replica**. v0 retains those rows
indefinitely. Replacing or deleting a key does not bound historical metadata;
budget by total logical write count as well as live data size. Stream/outbox
payload limits do not bound SQLite indexes, WAL, free pages or this ledger.

## Scope

This measures one owner namespace and two replicas on one Fedora host, over
loopback with a single file-backed JetStream server and recoverable disks. It
does not measure TLS across machines, Raft replication, failure during the timed
run, large objects, vectors, graphs, query workloads or production traffic. The
separate failure suite covers process crashes and policy exhaustion. No runtime
optimization or durability weakening was made to obtain these results.
