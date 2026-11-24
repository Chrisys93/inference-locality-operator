#!/usr/bin/env bash
# Rebalancing comparison on the simulators, arm R2 routing (bucket-affinity-scorer): buckets moved in index order
# (policy without scorerMetrics) against the busiest bucket that fits (scorerMetrics set). Zipf workload at a
# concurrency that makes the owner of the popular buckets queue past the trigger. The simulators are a StatefulSet
# (sim-0, sim-1), so every run starts from the same map; HOT_OFFSET picks which documents are popular, and so how
# skewed that start is (offset 1: sim-0 owns ~82% of requests, the busiest bucket ~32%; offset 8: ~90%, the five
# most popular documents all on sim-0). Before each run: fresh simulator caches and a fresh map. During each run
# the policy status is recorded every 10 s.
#   hack/sim-baseline/run-rebalance.sh results/rebalance-001
set -uo pipefail

OUT=${1:?results directory}
HERE=$(cd "$(dirname "$0")" && pwd)
NS=ilo-sim
POLICY=sim-pool
REPEATS=${REPEATS:-2}
REQUESTS=${REQUESTS:-12000}
CONCURRENCY=${CONCURRENCY:-16}
WORKLOAD=${WORKLOAD:-zipf}
OFFSETS=${OFFSETS:-"1 8"}
JOB_TIMEOUT=${JOB_TIMEOUT:-1200s}

mkdir -p "$OUT"
LOG="$OUT/run.log"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

set_arm() {
  case $1 in
    index) kubectl -n $NS patch prefixaffinitypolicy $POLICY --type json \
             -p '[{"op":"remove","path":"/spec/scorerMetrics"}]' >/dev/null 2>&1 || true ;;
    load)  kubectl -n $NS patch prefixaffinitypolicy $POLICY --type merge \
             -p '{"spec":{"scorerMetrics":{"service":"router-epp","auth":"ServiceAccountToken"}}}' >/dev/null ;;
  esac
}

fresh_start() {  # new simulator pods (empty caches), then a map built for them from scratch
  kubectl -n $NS rollout restart statefulset/sim >/dev/null &&
    kubectl -n $NS rollout status statefulset/sim --timeout=300s >/dev/null || return 1
  kubectl -n $NS delete configmap $POLICY-bucket-map --ignore-not-found >/dev/null
  local pods owners
  for _ in $(seq 1 90); do
    # ready pods not being deleted (old pods stay "Running" while they terminate)
    pods=$(kubectl -n $NS get pods -l app=sim-pool -o json | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
ready = [p["metadata"]["name"] for p in items if "deletionTimestamp" not in p["metadata"] and any(
    c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))]
print(" ".join(sorted(ready)))')
    owners=$(kubectl -n $NS get configmap $POLICY-bucket-map -o jsonpath='{.data.map\.json}' 2>/dev/null |
               python3 -c 'import json,sys; print(" ".join(sorted(set(json.load(sys.stdin)["owners"]))))' 2>/dev/null)
    [ -n "$pods" ] && [ "$owners" = "$pods" ] && return 0
    sleep 2
  done
  log "map owners [$owners] never matched ready pods [$pods]"
  return 1
}

watch_status() {  # policy status every 10 s, one JSON line each
  while true; do
    kubectl -n $NS get prefixaffinitypolicy $POLICY -o json 2>/dev/null |
      python3 -c 'import json,sys,time; s=json.load(sys.stdin)["status"]; s["t"]=time.time(); print(json.dumps(s))'
    sleep 10
  done
}

log "start: repeats=$REPEATS offsets=[$OFFSETS] requests=$REQUESTS concurrency=$CONCURRENCY workload=$WORKLOAD"
for off in $OFFSETS; do
for rep in $(seq 1 "$REPEATS"); do
  if (( rep % 2 )); then arms="index load"; else arms="load index"; fi
  for arm in $arms; do
    job="rb-$arm-o$off-$rep"
    set_arm "$arm"
    fresh_start || { log "fresh start failed before $job"; continue; }
    since=$(date -u +%FT%TZ)
    watch_status > "$OUT/$job.status.jsonl" &
    watcher=$!
    kubectl -n $NS delete job "$job" --ignore-not-found >/dev/null
    JOB=$job ARM="R2-$arm" TARGET=http://router-epp:8081 WORKLOAD=$WORKLOAD REQUESTS=$REQUESTS \
      CONCURRENCY=$CONCURRENCY SEED=$rep HOT_OFFSET=$off envsubst < "$HERE/loadgen-job.yaml.tpl" |
      kubectl apply -f - >/dev/null
    if kubectl -n $NS wait --for=condition=complete job/"$job" --timeout=$JOB_TIMEOUT >/dev/null; then
      kubectl -n $NS logs job/"$job" > "$OUT/$job.jsonl"
      log "done $job ($(grep -c '"type": "req"' "$OUT/$job.jsonl") requests)"
    else
      kubectl -n $NS logs job/"$job" > "$OUT/$job.failed.jsonl" 2>&1
      log "FAILED $job"
    fi
    kill $watcher 2>/dev/null
    kubectl -n inference-locality-operator-system logs deploy/inference-locality-operator-controller-manager \
      --since-time="$since" 2>/dev/null | grep -E 'bucket moved|no bucket can move' > "$OUT/$job.moves.log"
  done
done
done
set_arm load
log "finished"
