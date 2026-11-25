# Architecture

Status: design, 2026-11-19; simulator baseline (R0, R1) run 2026-11-23, see `PREP.md` step 2. Controller
(component 1) and scorer (component 2) implemented and checked on the simulators 2026-11-23; busiest-bucket
rebalancing compared on the simulators 2026-11-24; warm-up and `CachePolicy` are not written yet.

## The problem

In an [llm-d](https://llm-d.ai) deployment, the Endpoint Picker (EPP) scores every request against the model-server
replicas of an `InferencePool` (prefix-cache match, queue length, KV-cache use) and picks one. Two things are missing:

1. **Nobody owns the prefix space.** Prefix scoring follows wherever a prefix happens to be cached. Under a skewed
   workload the popular prefixes pile up on one replica; the queue scorer then spreads them out and the cache hits
   are lost. Neither component decides which replica *should* hold which prefixes, and nothing moves cached state
   to follow the load.
2. **State outside the GPU has no lifecycle.** Offloaded KV blocks, model checkpoints on node disks and retrieval
   indices are kept or dropped by LRU, with no policy on how long an item is worth keeping, or where it goes when
   it stops being worth keeping on the fast tier.

## Where the design comes from

Two earlier systems answer the same questions for data at the network edge:

- **SEND** — A.-C. Nicolaescu, S. Mastorakis, I. Psaras, *Store Edge Networked Data (SEND): A Data and Performance
  Driven Edge Storage Framework*, IEEE INFOCOM 2021. Edge Data Repositories (EDRs) store labelled data; a central
  controller receives periodic feedback (every 1,000 requests) and places data by popularity, by function or both.
  Every item has a **shelf life**: on expiry it is sent to the cloud or deleted, as its policy and metadata say.
- **ReStorEdge** — A.-C. Nicolaescu, S. Mastorakis, M. W. Al Azad, D. Griffin, M. Rio, *ReStorEdge: An edge
  computing system with reuse semantics*, arXiv:2405.17263, 2024. Data is grouped into locality-sensitive-hash
  **buckets**, each owned by one EDR and found through a forwarding table. An orchestrator **rebalances buckets,
  together with their data**, when a node is overloaded. Triggers compared: queueing delay above twice the mean
  service time, CPU, CPU plus workload, CPU plus reuse; rebalancing raised throughput by 25-33%.
- **Thesis** — A.-C. Nicolaescu, *Edge Networked Storage: Enabling Seamless Data Management, Processing and Reuse
  in Edge Networked Systems*, PhD thesis, University College London, 2023
  (https://discovery.ucl.ac.uk/id/eprint/10189608/). Extends both: SEND's placement strategies (general-purpose,
  popularity, function, and a hybrid that places by function proximity first and popularity otherwise, 4.4.4);
  ReStorEdge's orchestration algorithms (queue delay, CPU, epoch-based workload with expected loads updated after
  each move, CPU plus reuse; 5.2.5), a further workload study (5.4) and workload-based bucket splitting for a
  bucket more popular than a node's fair share (5.5).

The mapping to an inference pool:

| Edge system | Inference pool |
|---|---|
| Edge Data Repository | model-server replica (vLLM, SGLang) |
| data item | KV blocks of a prompt prefix |
| LSH bucket | prefix-hash bucket |
| forwarding table | bucket map read by an EPP scorer |
| orchestrator / controller | this operator |
| moving a bucket's data | warming the new owner's KV cache (replaying the bucket's prefixes; later, KV transfer through a vLLM KV connector) |
| feedback every N requests | reconcile interval over the replicas' metrics |
| shelf life, cloud or delete | `CachePolicy`: expiry archives an item to a slower tier or deletes it |
| popularity / function placement | where checkpoints, offloaded KV and indices are kept |

## Components

### 1. `PrefixAffinityPolicy` (`routing.chrisys93.github.io/v1alpha1`)

Implemented 2026-11-23 (`api/v1alpha1`, `internal/controller`, `internal/buckets`, `internal/metrics`). Sample:
`config/samples/routing_v1alpha1_prefixaffinitypolicy.yaml`.

```yaml
spec:
  selector: { matchLabels: { app: sim-pool } }   # model-server pods, as the InferencePool selects them
  metricsPort: 8000
  metricsPath: /metrics
  buckets: 64                         # prefix-hash buckets shared out among the replicas
  prefix:
    bytes: 2048                       # leading bytes of the prompt that define the bucket key
  trigger:
    type: QueueDelay                  # QueueDelay | KVCacheUsage
    queueDelayPercent: 200            # queueing delay > 200% of service time
    kvCacheUsagePercent: 90           # for KVCacheUsage
    minRequests: 20                   # fewer completed requests in the interval: replica not judged
  rebalance:
    interval: 30s
    maxMovesPerInterval: 4
    minSharePercent: 25               # a replica keeps at least 25% of its fair share of buckets
    cooldown: 2m                      # a moved bucket stays put this long
  scorerMetrics:                      # optional: the scorer's per-bucket counters, read through the EPP's Service
    service: router-epp
    port: 9090
    auth: ServiceAccountToken         # None | ServiceAccountToken (never sent to an ExternalName Service)
status:                               # mapVersion, totalMoves, lastRebalanceTime, Ready condition, and per replica:
  replicas: [ { name, buckets, requests, meanQueueMillis, meanServiceMillis, kvCacheUsagePercent, overloaded } ]
  bucketMap: <policy>-bucket-map      # ConfigMap, key map.json
  busiestBuckets: [ { bucket, owner, requests, cachedPercent, ownerServedPercent } ]   # with scorerMetrics
```

Each interval the controller:

1. lists the ready pods matching the selector;
2. reads each pod's `/metrics` and takes the difference from the previous reading: requests completed
   (`vllm:request_inference_time_seconds_count`), mean service time (its `_sum`) and mean queueing delay
   (`vllm:request_queue_time_seconds_sum`, divided by the completed requests, since a server may record queue time
   only for requests that waited), plus `vllm:kv_cache_usage_perc`; with `scorerMetrics`, also the scorer's
   per-bucket counters, differenced the same way. Reconciles caused by pod or map events between
   intervals do not read metrics, so the window stays one interval long;
3. keeps the map in step with the pods by rendezvous hashing: the first map gives each bucket to its highest-hash
   pod; a pod that leaves hands its buckets to their winners among the rest; a pod that joins takes the buckets it
   wins (about 1/n); buckets moved by rebalancing otherwise stay where they were moved;
4. when the trigger fires for a pod and some pod is readable and below its trigger, moves up to
   `maxMovesPerInterval` of the hot pod's buckets (not in cooldown), never below the `minSharePercent` floor.
   With per-bucket counts this is ReStorEdge's workload orchestration (thesis Alg. 4) inside its queueing trigger
   (Alg. 2): each pod's expected load is the sum of its buckets' requests; the busiest bucket goes first, to the
   pod with the least expected load, and moves only if that pod's expected load stays at or below the source's
   afterwards; expected loads are updated after every move. A bucket busier than half the gap stays where it is:
   moving it would only move the overload, and replicating it (the thesis' bucket splitting, 5.5) is the remedy.
   With `scorerMetrics` set but no counts for the interval (the first reading, or the scorer unreadable), nothing
   moves: a policy that asks for informed moves does not move blind. Without `scorerMetrics`, buckets move in index
   order to the coolest pods in turn;
5. publishes the map (`map.json`: version, hash name, bucket count, prefix bytes, owner per bucket, move times and
   count) in a ConfigMap owned by the policy, and patches the status.

Compared on the simulators (2026-11-24, run rebalance-004; arm R2s, so requests follow the map; Zipf workload,
12,000 requests at concurrency 16; `hack/sim-baseline/run-rebalance.sh`). The pods are a StatefulSet, so every run
starts from the same map, and the documents made popular are chosen so that one replica starts overloaded: with
offset 1 it owns ~82% of requests (busiest bucket ~32%), with offset 8 ~90% (the five most popular documents).
Both arms see the trigger fire (queueing 2.8-2.9x service time). Two runs per arm and offset:

| | moves per run | busiest replica, whole run | settled after | cached prompt tokens | p50 / p95 (sim) | requests/s (sim) |
|---|---|---|---|---|---|---|
| index order | 32-36 | 53-61% | ~2 min, then drifts (55-62%) | 94.0-94.5% | 0.45-0.47 / 0.76-0.79 s | 37.5 |
| busiest that fits | 3-4 | 50-56% | 1 interval (51-53%) | 94.7-94.8% | 0.32-0.34 / 0.64-0.70 s | 46.3 |

With the counts, one interval's moves were enough: either the ~32% bucket itself, or (when it did not fit by a few
requests) the next three busiest, which add up to about the same. Index order spent its moves on idle buckets
first and kept moving until the floor. The simulator's latencies are configured, so the latency and throughput
columns show that relief happened, not how much a GPU would gain (that is step 5); the 24% more requests per second
is in the range ReStorEdge reported (25-33%), which is a reason to measure, not a result. Results stay in
`results/` (not committed).

On the GPU (2026-11-25, run gpu-rebalance-002; `hack/gpu/run-gpu.sh`): two vLLM replicas of code-doc-assistant's
`lightest` tier (Phi-3.5-mini, 4-bit AWQ, 5,976 tokens of KV cache each) on one 12 GB card, the same Zipf workload
(1,500 requests at concurrency 16, 32 output tokens, ~1,800-token prompts), prefix caches emptied and the router
restarted before every run. Three arms: R1 (stock llm-d router), R2s with index-order moves, R2s with the busiest
bucket that fits. Two runs per arm and offset, means:

| | offset | busiest replica | cached prompt tokens | p50 | requests/s | moves per run |
|---|---|---|---|---|---|---|
| R1 stock router | 1 / 8 | 50% / 51% | 49% / 50% | 4.0 / 3.9 s | 3.12 / 3.99 | - |
| R2s index order | 1 / 8 | 68% / 61% | 61% / 60% | 3.6 / 3.5 s | 3.66 / 4.44 | 0-16 / 24-32 |
| R2s busiest that fits | 1 / 8 | 52% / 53% | 63% / 62% | 3.4 / 3.5 s | 4.02 / 4.63 | 2-3 / 5-12 |

The queueing trigger fired on real queues (up to 3.3x service time). Owning prefixes by bucket raised the cached
share by 12-14 points over the stock router, which balances load but splits documents across replicas; moving the
busiest bucket that fits then balanced load as well as the stock router (52-53% on the busiest replica) with a
fifth to a tenth of index order's moves, and gave the most requests per second: +29% over R1 at offset 1, +16% at
offset 8. Tail latency did not improve (p95 6.4-6.8 s against R1's 5.8-7.4 s in runs without router restarts).
Caveats: two runs per cell; the router pod restarted during some runs (node stalls on this single-node cluster),
failing 0-43 of 1,500 requests in 6 of 12 runs across all arms, which also inflates those runs' p95; one
index-order run recorded no moves although its replica was overloaded (to look into).

Not yet: warming a bucket's new owner before the move is published, replicating a bucket too busy for one
owner, weighing buckets by more than their request count (cached share, trend over intervals), and the HitRate
trigger.

Checked on the simulators (2026-11-23): with 20 concurrent requests sent straight to one replica (4 slots), its
queueing delay was about 4x its service time; the controller moved 4 buckets every 30 s down to the floor
(29 -> 8 of 64 buckets, 21 moves, map version 7), with no errors and 14 MiB of memory. That traffic bypassed the
router on purpose, so the moves could not relieve it; the run is what showed the floor was needed.

### 2. `bucket-affinity-scorer` (EPP plugin)

Implemented 2026-11-23 (`epp/`, a Go module of its own, so the router's dependencies stay out of the controller).
llm-d router v1.0.0 keeps its plugins in a process-wide registry, and its `main` only runs the runner; `epp/cmd/epp`
registers the scorer and runs the same runner, so the image is the stock EPP plus one plugin, with no fork.

- **Map:** watches the one ConfigMap named in its parameters (`configMap`, `namespace`; a Role limited to that
  name is enough) and keeps the last map that decodes and uses the same key function.
- **Score:** takes the prompt text (completions: the prompt; chat: `role\ncontent\n` per message, in order), its
  bucket with `buckets.Key` over the map's `prefixBytes`, and scores the owning pod 1, others 0. No map, or no text
  prompt: everyone 0, so the other scorers decide.
- **Counts:** on each completed response, per bucket: requests, requests served by the owner, prompt tokens,
  cached prompt tokens (`ilo_bucket_{requests,owner_served,prompt_tokens,cached_tokens}_total{bucket}` on the EPP
  metrics endpoint, which checks tokens). These are the per-bucket popularity the busiest-bucket selection needs.
- **Config, soft (arm R2):** the stock scorers plus this one, weight 3 like the prefix-cache scorer
  (`hack/sim-baseline/router-values-r2.yaml`); the plugin is registered as Alpha, so the EPP runs with
  `--allow-experimental-plugins`.
- **Config, strict (arm R2s):** weight 10, above what the other scorers can give a non-owner together (7), and
  the router's saturation filter relaxed (`hack/sim-baseline/router-values-r2-strict.yaml`). That filter
  (`utilization-detector`, added to every profile by default) drops a replica with 5 or more waiting requests or
  80% KV-cache use before any scorer runs, so under load it, not the weights, decides; in R2s it stays as a last
  resort (64 waiting requests, KV cache over 100%).

Checked on the simulators (2026-11-23), Zipf workload, 3,000 requests, map 33/31 buckets: every request was served
by its bucket's owner, each document stayed on one replica, 93% of prompt tokens came from cache (the scorer's
counters and the simulators' agree), and the busiest replica took 88% of requests (R1: 54-64%), while its
queueing delay stayed at 38% of service time, under the 200% trigger, so nothing moved. Strict ownership
concentrates popular buckets; whether that costs latency is what the trigger watches, and what the GPU runs measure.
Pod names are the owners, so a rolling restart of the model servers reassigns every bucket; the simulators are
therefore a StatefulSet (`sim-0`, `sim-1`), whose names, and so whose map, survive restarts.

Soft or strict is a real choice. Under load (Zipf, concurrency 16) R2 kept only ~70% of requests on their bucket's
owner: the saturation filter removed the busy owner, the queue and KV-cache scorers favoured the other replica, and
once that replica had served a document the prefix-cache scorer favoured it too. The router absorbed the overload
by spilling, at the cost of a second copy of the spilled prefixes (89% of prompt tokens cached against 93-94%), and
the controller's trigger never fired. R2s leaves overload to the controller instead, as ReStorEdge's forwarding
tables did.

### 3. `CachePolicy` (second API, after the routing work is measured)

Selects cached items by labels and metadata, places them (popularity, function or both) and gives each a shelf life;
on expiry `Archive` moves the item to a named slower tier and `Delete` drops it. First targets: offloaded KV blocks
(vLLM KV connectors), model checkpoints on node volumes, retrieval indices.

## Measurement

Workload: the evaluation set of [code-doc-assistant](https://github.com/Chrisys93/code-doc-assistant) (13 questions
over three repositories, two stages), replayed through the router by its harness, plus synthetic skewed workloads.

Arms, under identical machine conditions:

| Arm | Routing |
|---|---|
| R0 | plain Kubernetes Service (kube-proxy picks a pod at random) |
| R1 | stock EPP: prefix-cache and queue scorers |
| R2 | EPP plus `bucket-affinity-scorer` at weight 3 (soft: the router may spill), buckets rebalanced on queueing delay |
| R2s | as R2, weight 10 and the saturation filter relaxed (strict: overload left to rebalancing) |
| R3 | as R2, other triggers (KV-cache use, hit rate) |

Reported: prefix-cache hit rate (vLLM counters), time to first token and request latency (p50, p95), throughput,
queue time, number of moves and warm-up cost. The 25-33% from ReStorEdge is a figure to compare against, not one
assumed to carry over.

## Environments

- **Development:** `llm-d-inference-sim` replicas (no GPU). The simulator reports the metrics above and simulates a
  KV cache; the controller is unit-tested with envtest. Its latencies are configured, not computed, so simulator
  runs only show that the pieces work together; no performance figure is taken from them.
- **Measurement:** two vLLM replicas of a small model sharing one 12 GB GPU (about 0.40 of its memory each),
  on single-node minikube under WSL2.

Pinned versions: Gateway API Inference Extension v1.6.2 CRDs (`v1-manifests.yaml`); llm-d router v1.0.0
(`oci://ghcr.io/llm-d/charts/llm-d-router-standalone --version v1.0.0`, EPP image
`ghcr.io/llm-d/llm-d-router-endpoint-picker:v1.0.0`); `ghcr.io/llm-d/llm-d-inference-sim:v1.2.0`; Go 1.27.4;
kubebuilder v4.16.0; Kubernetes 1.37.

## Open questions

- **Bucket key.** Characters are simple and identical in controller, scorer and warmer; tokens match the KV block
  layout. For RAG prompts the shared part is the system prompt and template, so the key has to cover enough of it.
- **Warm-up cost.** Replaying prefixes costs compute on the new owner; KV transfer through a connector would move
  the cache itself.
- **Scorer interplay.** Ownership, prefix match and queue length can disagree; weights decide, and are measured.
- **Map delivery.** A watched ConfigMap is simplest; the EPP's own datastore would avoid a second source of truth.
