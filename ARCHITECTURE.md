# Architecture

Status: design, 2026-11-19. Only the kubebuilder scaffold exists (`PrefixAffinityPolicy` API and an empty
reconciler). Everything below is the plan; measured results will be added as they come.

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

Draft spec (to be settled in code):

```yaml
apiVersion: routing.chrisys93.github.io/v1alpha1
kind: PrefixAffinityPolicy
metadata:
  name: eval-pool
spec:
  poolRef: { name: vllm-pool }        # InferencePool, or modelServerSelector: { matchLabels: ... }
  buckets: 64                         # prefix-hash buckets shared out among the replicas
  prefix:
    chars: 2048                       # leading part of the prompt that defines the bucket key
  trigger:
    type: QueueDelay                  # QueueDelay | KVCacheUsage | HitRate
    queueDelayFactor: 2.0             # rebalance when queue time > factor x mean service time
    window: 60s
  rebalance:
    interval: 30s
    maxMovesPerInterval: 4
    cooldown: 2m
    warmup: { enabled: true, maxPrefixesPerBucket: 8 }
status:
  observedReplicas: [ ... ]           # per replica: queue time, service time, KV use, hit rate
  bucketMapRef: { name: eval-pool-bucket-map }   # ConfigMap: bucket -> replica
  lastRebalance: ...
  conditions: [ ... ]
```

The controller:

1. lists the pool's replicas;
2. scrapes each replica's `/metrics`: `vllm:request_queue_time_seconds` and `vllm:request_inference_time_seconds`
   (histograms, for queueing delay against service time), `vllm:num_requests_waiting`, `vllm:kv_cache_usage_perc`,
   `vllm:prefix_cache_hits_total` and `vllm:prefix_cache_queries_total`;
3. starts from an even split of buckets over replicas (consistent hashing, so adding a replica moves few buckets);
4. when the trigger fires for a replica, moves up to `maxMovesPerInterval` of its busiest buckets to the least
   loaded replica, warms the new owner first, then publishes the new map;
5. writes the map to a ConfigMap and the observations to `status`.

### 2. `bucket-affinity-scorer` (EPP plugin)

llm-d router v1.0.0 registers plugins in tree only (`plugin.Register` in `cmd/epp/runner/runner.go`), so the
scorer is built into a fork of the EPP image. It hashes the request's prefix exactly as the controller does, looks
up the owner in the bucket map (watched ConfigMap) and scores the owner 1, others 0. It runs next to the stock
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
