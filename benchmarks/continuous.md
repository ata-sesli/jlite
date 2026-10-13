# Continuous replication measurements

This experiment keeps owner publication and both replicas online throughout
ingestion. It complements the phased batching benchmark, which deliberately
accumulates a local backlog before publishing.

## Reproduce

```sh
sh scripts/podman-continuous-benchmark.sh
# Optional result destination and repetition count:
JLITE_BENCH_ROUNDS=3 sh scripts/podman-continuous-benchmark.sh benchmarks/results/repeated-continuous.json
```

The runner builds the Go 1.27.1 test image, runs the full race suite and vet, then
measures non-race binaries in a four-CPU, 2 GiB container on the configured Podman
remote. Each case runs in a fresh child process with new database/broker storage
on a dedicated volume. Networking is disabled; the authenticated broker runs
inside the container over loopback. The runner saves JSON and CPU/blocking
profiles and removes its own container and volume, including on failure.

## Workload and measurement

Each repetition offers 1 KiB keyed puts for approximately eight seconds at 20,
80 and 200 writes/second: 160, 640 and 1,600 operations respectively. Thirty-two
write workers use a bounded 64-item job queue. Actual achieved rates are recorded;
timer scheduling and blocked producers can reduce the offered rate. Eight seed
writes reach both replicas before measurement begins.

Both revisions use default limits: 500 operations/1 MiB/10 ms local batches,
1,024 operations/4 MiB admission, and 64 MiB outbox/stream payload budgets. The
publisher remains serial and synchronous. Both independent replica consumers
stay online with the existing conservative fetch bounds and 100 ms fetch wait.
SQLite uses WAL/FULL and the single file-backed JetStream server uses
`sync_interval: always`. No durability setting was relaxed.

The harness calls the writer, publisher and consumers directly; it does not
measure the Node retry/idle loop or control-socket overhead. An idle publisher
waits 1 ms between calls. Publisher interface wrappers record `Info`, `GetMsg`
and `Publish` call counts and wall time, without production timing hooks.
`PublisherTotal` includes idle inspections and cancellation of the final call;
its residual after subtracting broker calls includes local SQL, decoding,
scheduling and lock waits, not just checkpoint commits.

Progress snapshots during ingestion record committed, published and both applied
positions, plus outbox count/bytes. These are sequential reads, not a distributed
atomic snapshot. Outbox-byte/heap samples run every 10 ms; outbox-count peaks come
from the less frequent progress snapshots and can miss transient peaks. RSS is
the child process high-water mark, including native SQLite, the broker and all
three databases; Go heap excludes native allocations.

Local latency measures `Write` call to response. Publication/replica ages join
events by logical sequence after the drivers stop. Replica observation includes
consumer acknowledgements. An event observed before the caller receives its
local response gets a zero age. End-to-end throughput divides committed writes
by elapsed time until publication and both replicas finish, including drain;
it is not a steady-state capacity estimate. P95 summaries are medians of the
three per-run nearest-rank P95s.

Every sample verifies the complete final owner/replica prefix, retry/replica
ledger counts and every measured key/value. The capacity sampler checks the
configured admission and outbox bounds.

## Results

All 18 completed samples converged: all 14,400 measured writes were published
and applied by both replicas. The baseline matrix took 179.2 seconds and the
comparison took 171.4 seconds, excluding compilation and verification. The
timed runs were sequential; revision order was not alternated. Numbers below
are medians of three samples on the same shared host.

| Offered writes/s | End-to-end writes/s, before → after (ranges) | Local P95, before → after | Slower replica P95, before → after | Drain after ingestion, before → after |
| --- | --- | --- | --- | --- |
| 20 | 19.71 → 19.76 (19.60–19.85 → 19.66–19.76) | 23.18 → 24.69 ms | 124.50 → 126.48 ms | 0.10 → 0.08 s |
| 80 | 47.05 → 48.46 (45.13–47.71 → 46.96–49.25) | 36.13 → 38.22 ms | 5.37 → 5.02 s | 5.58 → 5.19 s |
| 200 | 50.59 → 54.17 (49.72–50.81 → 52.08–54.97) | 43.02 → 47.11 ms | 22.39 → 20.50 s | 23.59 → 21.48 s |

At 20/s, backlog stayed small and both revisions kept up over the eight-second
offer. At 80/s, median outbox count when ingestion finished was 302 in both
revisions; at 200/s it was 1,313 → 1,301. Those rates still accumulated backlog.
The measured 200/s end-to-end median improved 7.1%, but local P95 increased 9.5%.
This is a modest result with a latency tradeoff, not evidence of a new sustainable
200/s operating mode. Shared-host variation and grouped revision order limit
causal conclusions from the timing comparison.

Median process high-water RSS was 34.32 → 33.83 MiB at 20/s, 43.01 → 42.69 MiB at
80/s and 46.91 → 47.03 MiB at 200/s. Admission/outbox bounds held in every sample.
Median publisher message-read counts at 200/s fell from 4,856 to 3,230, including
idle inspections; the focused check establishes the per-call reduction exactly.

For the profiled baseline 200/s sample, publisher wall time was 31.83 seconds:
1.54 seconds in stream info, 3.54 in message reads, 10.60 in synchronous publish,
and 16.16 in the remaining local work/lock waits. The CPU profile shows checkpoint
commits and native work prominently. Removing one repeated read does not remove
the larger serial publication/checkpoint cost. Further substantial improvement
would require a separately tested change to commit grouping or publication flow;
this experiment does not implement that protocol change.

## Publisher change

When the retained tail is exactly the durable checkpoint, the previous publisher
fetched that same message once as the anchor and again as the tail. It now reuses
the verified anchor within that inspection. Normal publication requires two
message reads instead of three; idle verification requires one instead of two.
There is no cache across calls. Authentication, stream policy/identity checks,
unexpected-tail validation, accepted-message verification and atomic durable
checkpointing still run.

The focused read-count check failed before the change and passed afterward.
The full Podman race suite, including SIGKILL recovery and deduplication-window
expiry, passed after the change; vet passed too. An initial verification run
timed out at 90 seconds under concurrent load; the isolated run passed in
76.4 seconds with a three-minute allowance.

## Evidence and limits

The baseline runtime is commit `622e63d913e5efebdd1228081114aa91e06537b8`; the
comparison adds the five-line anchor reuse. The new benchmark was installed in
the existing test image for both runs. Between runs, only failure cleanup in
the harness was strengthened; successful workload and metric collection stayed
the same. The environment was Fedora 44/Linux amd64 on a shared physical host,
with a dedicated btrfs-backed volume and identical container limits.

- [Before raw results](results/continuous-before.json)
- [After raw results](results/continuous-after.json)
- [Environment and excluded attempt](results/continuous-environment.json)
- [Measured source checksums](results/continuous-source.sha256)
- [Baseline CPU profile](results/continuous-before.json.cpu)
- [After CPU profile](results/continuous-after.json.cpu)

The round-one 200/s case captures a CPU profile in each revision. Go CPU profiles
attribute native work largely to cgo and do not directly measure disk wait.
The captured blocking profile had no usable samples; it provides no evidence
about wait distribution. Broker-call wall timings provide the complementary
phase evidence. All three rounds are retained. An initial after run was stopped
because it overlapped with the race suite and is excluded as an invalid setup,
not a performance outlier.

These are short, synthetic, one-namespace runs with one broker and two replicas
on one host. They do not establish long-running stability, an application SLA,
TLS/network costs across hosts or Raft performance.
