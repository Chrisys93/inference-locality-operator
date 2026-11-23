# Architecture

Status: design, 2026-11-19; simulator baseline (R0, R1) run 2026-11-23, see `PREP.md` step 2. Controller
implemented and checked on the simulators 2026-11-23 (component 1); the scorer and `CachePolicy` are not written
yet.

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
status:                               # mapVersion, totalMoves, lastRebalanceTime, Ready condition, and per replica:
  replicas: [ { name, buckets, requests, meanQueueMillis, meanServiceMillis, kvCacheUsagePercent, overloaded } ]
  bucketMap: <policy>-bucket-map      # ConfigMap, key map.json
```

Each interval the controller:

1. lists the ready pods matching the selector;
2. reads each pod's `/metrics` and takes the difference from the previous reading: requests completed
   (`vllm:request_inference_time_seconds_count`), mean service time (its `_sum`) and mean queueing delay
   (`vllm:request_queue_time_seconds_sum`, divided by the completed requests, since a server may record queue time
   only for requests that waited), plus `vllm:kv_cache_usage_perc`. Reconciles caused by pod or map events between
   intervals do not read metrics, so the window stays one interval long;
3. keeps the map in step with the pods by rendezvous hashing: the first map gives each bucket to its highest-hash
   pod; a pod that leaves hands its buckets to their winners among the rest; a pod that joins takes the buckets it
   wins (about 1/n); buckets moved by rebalancing otherwise stay where they were moved;
4. when the trigger fires for a pod and some pod is readable and below its trigger, moves up to
   `maxMovesPerInterval` of the hot pod's buckets (not in cooldown) to the coolest pods in turn, never below the
   `minSharePercent` floor;
5. publishes the map (`map.json`: version, hash name, bucket count, prefix bytes, owner per bucket, move times and
   count) in a ConfigMap owned by the policy, and patches the status.

Not yet: choosing the busiest buckets to move (the logic takes per-bucket load, which only the scorer can supply),
warming a bucket's new owner before the move is published, and the HitRate trigger.

Checked on the simulators (2026-11-23): with 20 concurrent requests sent straight to one replica (4 slots), its
queueing delay was about 4x its service time; the controller moved 4 buckets every 30 s down to the floor
(29 -> 8 of 64 buckets, 21 moves, map version 7), with no errors and 14 MiB of memory. That traffic bypassed the
router on purpose, so the moves could not relieve it; the run is what showed the floor was needed.

### 2. `bucket-affinity-scorer` (EPP plugin)

llm-d router v1.0.0 registers plugins in tree only (`plugin.Register` in `cmd/epp/runner/runner.go`), so the
scorer is built into a fork of the EPP image. It hashes the request's prefix exactly as the controller does (`buckets.Key`
over the map's `prefixBytes`, imported from this module), looks up the owner in the bucket map (watched ConfigMap) and scores the owner 1, others 0. It runs next to the stock
`prefix-cache-scorer` and `queue-scorer` with configurable weights, so that ownership guides placement without
overriding the queue when a replica is saturated.

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
| R2 | EPP plus `bucket-affinity-scorer`, buckets rebalanced on queueing delay |
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
