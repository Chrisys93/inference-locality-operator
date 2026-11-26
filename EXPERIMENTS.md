# Experiments

What the next measurements test, and why. The setting is a best-effort inference service: users neither see nor tune
how it is optimised, so the system has to find good locality by itself and keep it while the workload changes. The
question is therefore not only how well a policy does in steady state but **how fast, and at what cost, it reaches
locality again after the workload moves**, and whether it gets there by changing where *some requests* go rather
than reshuffling state across the whole pool. These are the questions SEND (placement by popularity, with shelf
lives) and ReStorEdge (bucket ownership, rebalancing *with the data* under overload) answer for edge storage; the
thesis (Ch. 4-5) gives the algorithms.

Baseline: the stock llm-d router (arm R1) as shipped. It already scores prefix-cache affinity, so it is the
prefix-aware router a deployment gets without anyone tuning it; a tuned llm-d is a later reference point, not the
comparison, since tuning is exactly what a best-effort service cannot rely on.

## Workloads

Requests are RAG-shaped prompts (shared preamble, one of 32 documents, a unique question), so prefix reuse comes from
document popularity, as in the papers:

- **zipf**: Zipf popularity (s = 1.2) over a fixed ranking; `HOT_OFFSET` sets how skewed the start is (used in
  gpu-rebalance-002).
- **shift**: the same Zipf, but the ranking rotates by a quarter of the documents every quarter of the run: the
  popular set moves three times. This is the timeliness axis: what was hot goes cold, other data becomes hot.
- **Arrivals**: open loop, Poisson at a fixed rate (`RATE`), as users arrive independently of how fast the service
  answers. Closed-loop concurrency (gpu-rebalance-002) hides overload: a slow replica simply receives fewer requests.
  The rate is set below the pool's capacity but above what one replica can take for the popular set.

## Answer quality, for every experiment

Locality is only worth having at the same answer quality, so every result reports quality next to throughput and
latency. Exact prefix reuse should not change an answer (the same tokens give the same KV), but cached and uncached
prefills take different kernels and batch shapes, so greedy decoding can occasionally diverge; and a policy can change
answers indirectly, through failed requests, timeouts or re-plans under load in an adaptive pipeline. Approximate
reuse (blending, semantic caches, lower-precision KV, KV transfer) would make quality the main trade-off. Measured as:

- **E0, invariance**: a fixed prompt set at temperature 0, prefix cache on and off, and through each arm: the share of
  identical outputs, and where they differ, the first differing token and the length of the divergence;
- **end-to-end quality** on the real task: the code-doc-assistant evaluation set (`eval/run_eval.py`) through each
  arm, two to three runs each: automatic checks, retrieval metrics and the LLM judge;
- **failed and degraded answers**: errors, timeouts and re-plans count against the arm that caused them.

The synthetic workload (random-word documents, 32 output tokens) measures load and locality only, not quality.

## E1 Convergence after a popularity shift (no new mechanism)

Arms R1, R2s-index, R2s-load (as in gpu-rebalance-002); workload shift, open loop. Per 30-s interval and per phase:

- **time to converge** after each shift: intervals until the busiest replica's share stays at or below 60% and the
  cached share of prompt tokens is back within 5 points of the phase's steady state;
- **cost of converging**: buckets moved, requests served by a non-owner, cold prefills (prompt tokens not served
  from cache) in the minute after a shift;
- latency p50 / p95 overall and in the minute after each shift; errors.

Expected, if the papers carry over: R2s-load converges in one or two intervals with few moves; R2s-index needs many
moves and may not converge; R1 stays balanced throughout but at a lower cached share, and re-spreads every new hot
document over both replicas.

## E2 Moving the data with the bucket: warming the new owner

ReStorEdge moves a bucket *together with its data*; so far a moved bucket starts cold on its new owner, and its
first requests there recompute their prefixes. The scorer sees every prompt, so it can keep the last few prompts of
each bucket and, when the map moves a bucket, replay them to the new owner (one output token each) **while it keeps
routing that bucket to the old owner**; once the replay is done, the bucket's requests go to the new owner. Only the
routing of that one bucket's requests changes, and only after the data is in place. Prompts never leave the router.

Arms: R2s-load against R2s-load with warming; workload shift. Measured as in E1, plus the cached share and latency of
the moved buckets' requests in the interval after each move. Expected: the cold-prefill penalty after a move
disappears, and with it part of the tail latency R2s-load did not improve in gpu-rebalance-002.

## E3 A bucket too popular for one owner (thesis 5.5)

When the busiest bucket alone exceeds what one replica can serve (it "does not fit"), rebalancing only moves the
overload. The thesis splits such a bucket; here, the bucket gets a second owner and the scorer spreads its requests
over both (warmed as in E2). Arms: R2s-load with and without replication; workload zipf with a dominant document.
Later, after E1 and E2.

## E4 Similar wording, different context

Prompts that share wording but not context are where reuse can go wrong. For exact prefix reuse the risk is to
routing, not to answers: KV is reused only for an identical leading run of tokens, so answers stay correct, but the
bucket key (the first `prefixBytes` of the prompt) groups prompts by whatever leads. With a shared template first,
prompts about different contexts share a bucket and a replica (load concentrates with little reuse: false locality);
with the context first, one context reached through different wording splits across buckets. For approximate reuse
(semantic caches, KV blending, ReStorEdge's reuse of computations on similar inputs) the risk is to the answer itself:
the state or answer of the wrong context, plausible but wrong.

Workload with checkable answers: synthetic factual documents, each stating its own facts ("in document k the
service's port is p_k ..."), and the same questions asked of every document; an answer is correct only if it matches
its own document's fact (exact match, no judge). Varied: layout (question first or context first, template length),
wording (identical, paraphrased, unrelated), and later the reuse mechanism (exact now; approximate if added).
Measured: whether buckets follow context or wording, the cache hits that happened, load concentration, and **the share
of answers correct for their own context**: 100% expected in every arm with exact reuse; with approximate reuse, the
trade-off curve against the reuse gained.

**Keying, as the hypothesis to test.** Two levels, each doing one job:

- **model fingerprint, for correctness**: KV state is valid only for the model that produced it (weights, tokenizer,
  quantization, precision), so it is partitioned by model; this is where exactness comes from;
- **context identity, for locality**: requests about the same context are placed on the same owner, which is what
  produces exact prefix hits. Best taken from identity when the application knows it (the retrieved document or chunk
  IDs of a RAG request), otherwise from an embedding of the *context part* of the prompt (an embedding of the whole
  prompt is dominated by shared wording, the same failure as the byte-prefix key). Similarity is used only where reuse
  is deliberately approximate, where its threshold traces the correctness curve. The embedding's cost per request is
  measured with it.

Arms for E4: byte-prefix key (now), context-ID key, context-embedding key; layouts and wordings as above.

## E5 Heterogeneity: hardware, precision, models

Exact reuse assumes the same model on the same numerics. Across GPU types, kernels or precisions the same model gives
slightly different KV (numerically approximate reuse); across fine-tunes of one base model, closer but not equal state;
across different models, none without a learned mapping (as projectors map vision embeddings into a language model's
input space, but for attention state, and lossy by construction). Each step makes every placement decision carry a
quality cost as well as a time cost. Measured as E4's correctness share and output divergence, against the reuse each
step allows. Needs hardware beyond this machine for the hardware axis; precision (fp16 against fp8 KV) and fine-tuned
variants are possible here.

## Common method

Two vLLM replicas of code-doc-assistant's `lightest` tier on one 12 GB GPU (`hack/gpu/`); prefix caches emptied and
the router restarted before every run; three runs per arm and condition, arms in rotating order; the driver stops if
a replica restarts. The code-doc-assistant stack and the simulators are scaled down during runs, the same for every
arm. Known limits of this machine, stated with every result: the replicas share one GPU (they are not independent),
the router pod restarts during some runs (node stalls), and the model is small.
