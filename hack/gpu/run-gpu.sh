#!/usr/bin/env bash
# Routing and rebalancing on the GPU: two vLLM replicas of the lightest tier (hack/gpu/vllm.yaml). Three arms:
#   R1         stock llm-d router (queue, KV-cache and prefix-cache scorers); the operator does not move buckets
#   R2s-index  bucket-affinity-scorer, strict; buckets move in index order (policy without scorerMetrics)
#   R2s-load   bucket-affinity-scorer, strict; the busiest bucket that fits moves first (scorerMetrics set)
# WORKLOAD (zipf, shift; EXPERIMENTS.md), closed loop or open loop at RATE. Zipf: HOT_OFFSET picks which documents are popular and so how skewed the map's start is (offset 1: ~82%
# of requests on vllm-0). Before each run: both replicas' prefix caches emptied (POST /reset_prefix_cache, vLLM dev
# mode; the replicas stay loaded, since a reload from disk took ~10 min on this machine), the router restarted (empty
# prefix index), and a fresh map. During each run the policy status is recorded every 10 s; before and after it, each
# replica's vLLM counters (vLLM returns no pod header, so the split per replica comes from their difference).
#   hack/gpu/run-gpu.sh results/gpu-rebalance-001
set -uo pipefail

OUT=${1:?results directory}
HERE=$(cd "$(dirname "$0")" && pwd)
SIM=$HERE/../sim-baseline
NS=ilo-gpu
POLICY=gpu-pool
MODEL=${MODEL:-phi3.5:3.8b-mini-instruct-q4_K_M}
REPEATS=${REPEATS:-2}
REQUESTS=${REQUESTS:-1500}
CONCURRENCY=${CONCURRENCY:-16}
MAX_TOKENS=${MAX_TOKENS:-32}
WORKLOAD=${WORKLOAD:-zipf}
RATE=${RATE:-}           # requests/s, open loop (Poisson); empty = closed loop at CONCURRENCY
PROMPTS=${PROMPTS:-rag}  # rag | facts (EXPERIMENTS.md E4: checkable answers)
LAYOUT=${LAYOUT:-context_first}
WORDING=${WORDING:-identical}
OFFSETS=${OFFSETS:-"1 8"}
ARMS=${ARMS:-"R1 R2s-index R2s-load"}
JOB_TIMEOUT=${JOB_TIMEOUT:-900s}
CHART=oci://ghcr.io/llm-d/charts/llm-d-router-standalone

mkdir -p "$OUT"
LOG="$OUT/run.log"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

ROUTER=""
set_router() {  # R1 | R2s; helm upgrade only when it changes
  [ "$ROUTER" = "$1" ] && return 0
  local extra=()
  [ "$1" = R2s ] && extra=(-f "$HERE/router-values-r2-strict.yaml")
  helm upgrade router $CHART --version v1.0.0 -n $NS -f "$HERE/router-values.yaml" "${extra[@]}" \
    --wait --timeout 300s >/dev/null || return 1
  ROUTER=$1
}

set_policy() {  # R1: no moves; R2s-index: moves, no counts; R2s-load: moves by counts
  local moves=4 scorer=0
  case $1 in R1) moves=0 ;; R2s-load) scorer=1 ;; esac
  kubectl -n $NS patch prefixaffinitypolicy $POLICY --type merge \
    -p "{\"spec\":{\"rebalance\":{\"maxMovesPerInterval\":$moves}}}" >/dev/null || return 1
  if [ $scorer = 1 ]; then
    kubectl -n $NS patch prefixaffinitypolicy $POLICY --type merge \
      -p '{"spec":{"scorerMetrics":{"service":"router-epp","auth":"ServiceAccountToken"}}}' >/dev/null
  else
    kubectl -n $NS patch prefixaffinitypolicy $POLICY --type json \
      -p '[{"op":"remove","path":"/spec/scorerMetrics"}]' >/dev/null 2>&1 || true
  fi
}

reset_caches() {  # empty both replicas' prefix caches (vLLM dev mode); retried, since kubectl exec on this node can
  local p try      # time out for a while after a router switch
  for p in vllm-0 vllm-1; do
    for try in 1 2 3 4 5; do
      timeout 90 kubectl -n $NS exec $p -- python3 -c '
import urllib.request
r = urllib.request.urlopen(urllib.request.Request("http://localhost:8000/reset_prefix_cache", data=b"", method="POST"),
                           timeout=30)
assert r.status == 200, r.status' && continue 2
      sleep 15
    done
    return 1
  done
}

fresh_start() {  # empty prefix caches, restarted router, a map built from scratch
  reset_caches || { log "prefix-cache reset failed"; return 1; }
  kubectl -n $NS rollout restart deployment/router-epp >/dev/null &&
    kubectl -n $NS rollout status deployment/router-epp --timeout=300s >/dev/null || return 1
  kubectl -n $NS delete configmap $POLICY-bucket-map --ignore-not-found >/dev/null
  local pods owners
  for _ in $(seq 1 90); do
    pods=$(kubectl -n $NS get pods -l app=vllm-pool -o json | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
ready = [p["metadata"]["name"] for p in items if "deletionTimestamp" not in p["metadata"] and any(
    c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))]
print(" ".join(sorted(ready)))')
    owners=$(kubectl -n $NS get configmap $POLICY-bucket-map -o jsonpath='{.data.map\.json}' 2>/dev/null |
               python3 -c 'import json,sys; print(" ".join(sorted(set(json.load(sys.stdin)["owners"]))))' 2>/dev/null)
    [ "$pods" = "vllm-0 vllm-1" ] && [ "$owners" = "$pods" ] && return 0
    sleep 2
  done
  log "map owners [$owners] never matched ready pods [$pods]"
  return 1
}

watch_status() {
  while true; do
    kubectl -n $NS get prefixaffinitypolicy $POLICY -o json 2>/dev/null |
      python3 -c 'import json,sys,time; s=json.load(sys.stdin)["status"]; s["t"]=time.time(); print(json.dumps(s))'
    sleep 10
  done
}

vllm_counters() {  # one JSON line per replica: totals since its start
  for p in vllm-0 vllm-1; do
    kubectl -n $NS exec $p -- python3 -c '
import json, sys, urllib.request
t = urllib.request.urlopen("http://localhost:8000/metrics").read().decode()
def g(n):
    return sum(float(l.split()[-1]) for l in t.splitlines() if l.startswith(n + "{") or l.startswith(n + " "))
print(json.dumps({"pod": sys.argv[1], "requests": g("vllm:request_success_total"),
    "prompt_tokens": g("vllm:prompt_tokens_total"), "prefix_hits": g("vllm:prefix_cache_hits_total"),
    "prefix_queries": g("vllm:prefix_cache_queries_total"), "preemptions": g("vllm:num_preemptions_total"),
    "queue_s": g("vllm:request_queue_time_seconds_sum"), "inference_s": g("vllm:request_inference_time_seconds_sum")}))' $p
  done
}

replica_state() {  # "<ready count> <restart total>" of the two replicas
  kubectl -n $NS get pods -l app=vllm-pool -o json | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
cs = [c for p in items for c in p["status"].get("containerStatuses", [])]
print(sum(c["ready"] for c in cs), sum(c["restartCount"] for c in cs))'
}

replicas_unchanged() {  # stop the whole comparison when a replica restarted or is not ready: a reload took up to
  local now                # 10 min on this machine and failed 35 times in a row overnight, so it is not retried here
  now=$(replica_state)
  [ "$now" = "2 $RESTARTS0" ] && return 0
  log "STOP: replicas changed (ready, restarts now: $now; at start: 2 $RESTARTS0)"
  return 1
}

read -r ready0 RESTARTS0 <<<"$(replica_state)"
[ "$ready0" = 2 ] || { log "STOP: $ready0 of 2 replicas ready at start"; exit 1; }
kubectl -n $NS create configmap loadgen --from-file="$SIM/loadgen.py" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
log "start: arms=[$ARMS] repeats=$REPEATS offsets=[$OFFSETS] workload=$WORKLOAD rate=${RATE:-closed} prompts=$PROMPTS layout=$LAYOUT wording=$WORDING requests=$REQUESTS concurrency=$CONCURRENCY max_tokens=$MAX_TOKENS"
for off in $OFFSETS; do
for rep in $(seq 1 "$REPEATS"); do
  arms=$ARMS
  (( rep % 2 )) || arms=$(echo $ARMS | tr ' ' '\n' | tac | tr '\n' ' ')   # reverse the order on even repeats
  for arm in $arms; do
    job="gpu-$(echo $arm | tr 'A-Z' 'a-z')-o$off-$rep"
    replicas_unchanged || exit 1
    case $arm in R1) set_router R1 ;; *) set_router R2s ;; esac || { log "router switch failed before $job"; continue; }
    set_policy "$arm" || { log "policy patch failed before $job"; continue; }
    fresh_start || { log "fresh start failed before $job"; continue; }
    since=$(date -u +%FT%TZ)
    vllm_counters > "$OUT/$job.vllm-before.jsonl"
    watch_status > "$OUT/$job.status.jsonl" &
    watcher=$!
    kubectl -n $NS delete job "$job" --ignore-not-found >/dev/null
    JOB=$job ARM=$arm TARGET=http://router-epp:8081 WORKLOAD=$WORKLOAD REQUESTS=$REQUESTS CONCURRENCY=$CONCURRENCY \
      SEED=$rep HOT_OFFSET=$off MODEL=$MODEL MAX_TOKENS=$MAX_TOKENS RATE=$RATE \
      PROMPTS=$PROMPTS LAYOUT=$LAYOUT WORDING=$WORDING envsubst < "$SIM/loadgen-job.yaml.tpl" |
      sed "s/namespace: ilo-sim/namespace: $NS/" | kubectl apply -f - >/dev/null
    if kubectl -n $NS wait --for=condition=complete job/"$job" --timeout=$JOB_TIMEOUT >/dev/null; then
      kubectl -n $NS logs job/"$job" > "$OUT/$job.jsonl"
      log "done $job ($(grep -c '"type": "req"' "$OUT/$job.jsonl") requests)"
    else
      kubectl -n $NS logs job/"$job" > "$OUT/$job.failed.jsonl" 2>&1
      kubectl -n $NS delete job "$job" --ignore-not-found >/dev/null
      log "FAILED $job"
    fi
    kill $watcher 2>/dev/null
    vllm_counters > "$OUT/$job.vllm.jsonl"
    kubectl -n inference-locality-operator-system logs deploy/inference-locality-operator-controller-manager \
      --since-time="$since" 2>/dev/null | grep -E 'bucket moved|no bucket can move' > "$OUT/$job.moves.log"
  done
done
done
set_policy R2s-load
log "finished"
