# Publication and checkpoint batching

The checkpoint-batching implementation measured here used `PublishBatch` to pipeline a bounded, ordered outbox prefix and
checkpoint that group in one SQLite transaction. The single-entry `PublishNext`
API remains available as a reference and for explicit caller scheduling.
It advances at most one logical checkpoint per call; recovery can confirm a
newer retained tail before reclaiming that oldest outbox entry.

This window-barrier publisher is the current implementation. A later
[continuous refill experiment](continuous-pipeline.md) was not adopted.

## Ordering and recovery

Each send retains its immutable change ID and carries the expected physical
JetStream sequence. The publisher uses one connection, disables SDK publication
retries, checks every ACK's stream and exact sequence, and verifies every stored
message's subject, ID and bytes before checkpointing. The wire format and stream
policy remain unchanged. SQLite still uses WAL/FULL; the test broker still uses
file storage with `sync_interval: always`.

Groups use the existing namespace limits, defaulting to 500 operations and 1 MiB
of encoded payloads. Only available work is collected; publication adds no linger.
The in-flight window is conservatively bounded by operation count and the byte
budget divided by maximum wire size: eight messages by default. Each window is
acknowledged and verified before sending the next, while the entire group still
uses one checkpoint transaction. Broker/ACK waits have individual request
timeouts; the caller context bounds total batch duration.

Outbox deletion, byte accounting, logical publication progress and the physical
stream checkpoint commit together. Errors report zero checkpointed operations.
An uncertain async send closes its connection and leaves the complete
uncheckpointed outbox for reconnection. The broker may already contain a prefix.

Recovery checks every message after the saved physical checkpoint against the
outbox, including messages in the middle of that suffix. It then retries only
the verified tail to obtain a durable publication ACK; read-back alone cannot
reclaim the outbox. If broker deduplication has expired, this can append an
identical tail duplicate. Logical order remains nondecreasing and replicas apply
the logical change once. Recovery never checkpoints beyond the physical prefix
confirmed by that ACK. A bounded recovered prefix is saved before new sends.

This is a publication optimization, not an atomic transaction across replicas.
Replicas may apply individual messages before the owner's group checkpoint is
saved. `PublishedSequence` describes saved local publication progress.

## Reproduce and compare

```sh
sh scripts/podman-continuous-benchmark.sh
```

The runner builds and verifies Go 1.27.1 on the configured Podman remote, then
compares `single` and `batch` publication in isolated non-race child processes.
It keeps local write batching and both replica consumers unchanged. Each mode
offers 1 KiB values for approximately eight seconds at 20, 80 and 200 writes/s,
with 32 write workers and a 64-item job queue. Three repetitions reverse mode
order in round two. The container has four CPUs, `GOMAXPROCS=4`, 2 GiB RAM,
networking disabled, and fresh storage per child on its own data volume.

`EndToEndOpsPerSecond` includes post-offer drain until publication and both
replicas finish. It is a finite-workload measurement, not steady-state capacity.
Publication-age observations mark group checkpoint completion, so early members
of a group may have reached JetStream or replicas before that timestamp.
Successful publisher calls count checkpoints; a real SQL trigger in the focused
tests separately verifies that one group produces one checkpoint transaction.
`AsyncEnqueue` records enqueue time only; it excludes ACK waits and cannot be
subtracted from `PublisherTotal` as though it were synchronous publish time.
CPU profiles include warm-up, validation and cleanup as well as ingestion/drain.

The old [continuous results](continuous.md) remain a historical comparison.
The new paired single-entry samples are the controlled reference on the current
host. The [phased batching results](README.md) measured local burst commits and
then backlog drain; their 1,912 local writes/s is not comparable to end-to-end
replicated throughput.

## Results

The October 10, 2026 run completed all 18 samples in 280.14 seconds, excluding
image build and verification. All 14,400 measured writes committed, published and
reached both replicas with matching records and logical ledgers. Every sample
returned to an empty outbox and preserved configured bounds.

These are medians of three samples. Replica P95 uses the slower replica's P95
in each sample; ranges retain every observed end-to-end throughput result.

| Offered writes/s | End-to-end writes/s, single → batch (ranges) | Local P95, single → batch | Replica P95, single → batch | Drain after ingestion, single → batch |
| --- | --- | --- | --- | --- |
| 20 | 19.75 → 19.71 (19.66–19.81 → 19.54–19.81) | 20.79 → 22.32 ms | 124.28 → 124.91 ms | 81 → 101 ms |
| 80 | 47.80 → 78.42 (46.98–51.34 → 78.38–78.61) | 50.92 → 53.96 ms | 5.18 → 0.40 s | 5.37 → 0.14 s |
| 200 | 52.82 → 117.47 (51.25–54.41 → 115.04–120.73) | 59.36 → 69.41 ms | 21.30 → 5.20 s | 22.24 → 5.36 s |

At 80/s, batched publication stayed close to the offered rate over the tested
interval. Median pending outbox count at ingestion completion fell from 295 to
4, and replica P95 fell about 92%. At 200/s, end-to-end throughput improved
2.22× and drain time fell about 76%, but pending outbox count was still 863
(versus 1,303 with single publication). The new path still did not sustain the
200/s offer. Outbox count includes accepted messages whose group checkpoint has
not yet completed; it is not identical to replica lag.

Local write P95 increased about 6% at 80/s and 17% at 200/s. Local ingestion
throughput at 200/s was 198.47 → 195.61 writes/s. The result improves propagation
and reduces commit overhead while exposing a local latency tradeoff. There was
no benefit at the 20/s offer, where groups were almost always one operation.

| Offered writes/s | Checkpoint calls, single → batch | Median maximum batch size, batch | Median RSS high-water, single → batch | Median Go heap peak, single → batch |
| --- | --- | --- | --- | --- |
| 20 | 160 → 159 | 2 | 35.15 → 34.76 MiB | 5.54 → 5.53 MiB |
| 80 | 640 → 156 | 25 | 43.01 → 43.49 MiB | 13.07 → 13.01 MiB |
| 200 | 1,600 → 12 | 500 | 48.29 → 49.18 MiB | 12.96 → 15.61 MiB |

The default in-flight cap remained eight in every sample, including the
500-operation checkpoint groups. High-rate checkpoint calls fell about 99.3%.
RSS includes the broker, three SQLite databases and verification bookkeeping;
Go heap excludes native allocations. These figures do not establish a process
memory ceiling.

The previous continuous benchmark's 200/s offer measured 54.17 end-to-end
writes/s, 21.48 seconds of drain and 20.50 seconds of replica P95 after the small
anchor-reuse optimization. The new batch path measured 117.47, 5.36 and 5.20
respectively. That historical comparison agrees with the controlled reference,
but it was a separate run; the fresh paired single/batch samples are stronger
evidence for this change.

Evidence:

- [All 18 raw samples](results/publication-batches.json)
- [Remote environment and image identity](results/publication-batches-environment.json)
- [Measured source checksums](results/publication-batches-source.sha256)
- [Single-publication CPU profile](results/publication-batches.json.single.cpu)
- [Batched-publication CPU profile](results/publication-batches.json.batch.cpu)
- [Initial failed implementation's partial matrix](results/publication-batches-initial.json.partial)

The measured source was `012bb0a5ebe86b61a4ac97567273cc0a8114c43f` plus the
uncommitted batching changes identified by the checksum file. The runner built
image `0d0f6f8a4100318317242cbd559677ab5a74d42c1fd8ed08511e77db8ed5f0f3`.
Full race tests and vet passed in that image before measurement
(`jlite` race suite 34.934 seconds). Timing was sequential, with no competing test
jobs on the container; physical CPU/storage remain shared with other host work.

## Correctness evidence

Tests use real Zova databases and authenticated JetStream. They cover count/byte
bounds, ordered stored messages, exactly one checkpoint per group, Node adoption,
partial send failure, cancellation before send and after real ACKs, refusal to
checkpoint without a tail ACK, corruption in the middle of a retained suffix,
and recovery with per-request timeouts through both public APIs.

SIGKILL barriers test broker acceptance before checkpointing and restart after
the checkpoint commits. Recovery occurs after the broker deduplication window
expires; stable write retries preserve their original results, and both replicas
converge with matching records and logical ledgers. The existing failure suite
continues to cover stream exhaustion, authorization, missing/recreated history,
replica transaction failure and offline replay.

The fresh Podman image runs the full race suite and vet before timing. Tests
cover process crashes with recoverable disks, not disk loss, correlated power
failure or a multi-server Raft deployment. The performance workload is short,
synthetic, one namespace and one broker over loopback on a shared Fedora host.

The initial whole-group enqueue implementation failed the 200/s case with a
replica sequence gap; diagnostic runs also exposed request timeouts. Its partial
matrix is retained separately and excluded from the final comparison. The
conservative rolling window replaced that implementation; three repeats of the
formerly failing high-rate case passed before the final matrix was started.
No gap, identity, acknowledgement or durability check was relaxed to obtain a
successful run.
