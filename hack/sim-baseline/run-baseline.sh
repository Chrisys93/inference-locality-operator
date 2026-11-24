#!/usr/bin/env bash
# Simulator baseline: arms R0 (plain Service) and R1 (stock llm-d router) on the same two simulator pods.
#
# Part 1, comparison: for each repeat, workload and arm (arms interleaved), restart the simulators (empty caches) and
# the EPP (empty prefix index), run one load-generator Job, keep its output.
# Part 2, soak: R1 under the shifting workload for SOAK_CHUNKS consecutive Jobs without restarts; pod memory and
# restarts sampled after every chunk.
#
# Usage: hack/sim-baseline/run-baseline.sh <results-dir>
# Expects: namespace ilo-sim with sims.yaml applied, the router chart installed as "router", ConfigMap "loadgen".
set -uo pipefail

OUT=${1:?results directory}
HERE=$(cd "$(dirname "$0")" && pwd)
NS=ilo-sim
REPEATS=${REPEATS:-5}
REQUESTS=${REQUESTS:-3000}
CONCURRENCY=${CONCURRENCY:-6}
WORKLOADS=${WORKLOADS:-"uniform zipf shift"}
SOAK_CHUNKS=${SOAK_CHUNKS:-10}
SOAK_REQUESTS=${SOAK_REQUESTS:-15000}
JOB_TIMEOUT=${JOB_TIMEOUT:-1500s}   # a phase takes ~3-4 min, a soak chunk ~15-20 min
MAX_FAILURES=${MAX_FAILURES:-3}

mkdir -p "$OUT"
LOG="$OUT/run.log"
failures=0

log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

declare -A TARGETS=([R0]=http://sim-plain:8000 [R1]=http://router-epp:8081)

restart() {  # kind/name
  kubectl -n $NS rollout restart "$1" >/dev/null &&
    kubectl -n $NS rollout status "$1" --timeout=300s >/dev/null
}

snapshot() {  # pod memory and restarts
  {
    echo "== $(date -u +%FT%TZ) $1"
    kubectl -n $NS top pods --containers 2>&1
    kubectl -n $NS get pods -o custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[*].restartCount 2>&1
  } >> "$OUT/pods.log"
}

run_job() {  # name arm workload requests seed
  local job=$1 arm=$2 workload=$3 requests=$4 seed=$5
  kubectl -n $NS delete job "$job" --ignore-not-found >/dev/null
  JOB=$job ARM=$arm TARGET=${TARGETS[$arm]} WORKLOAD=$workload REQUESTS=$requests CONCURRENCY=$CONCURRENCY SEED=$seed HOT_OFFSET=0 \
    envsubst < "$HERE/loadgen-job.yaml.tpl" | kubectl apply -f - >/dev/null || return 1
  if kubectl -n $NS wait --for=condition=complete job/"$job" --timeout=$JOB_TIMEOUT >/dev/null; then
    kubectl -n $NS logs job/"$job" > "$OUT/$job.jsonl"
    log "done $job ($(grep -c '"type": "req"' "$OUT/$job.jsonl") requests)"
  else
    kubectl -n $NS logs job/"$job" > "$OUT/$job.failed.jsonl" 2>&1
    log "FAILED $job"
    return 1
  fi
}

fail_check() {
  failures=$((failures + 1))
  if (( failures >= MAX_FAILURES )); then
    log "stopping: $failures failures"
    snapshot "stopped"
    exit 1
  fi
}

log "start: repeats=$REPEATS requests=$REQUESTS concurrency=$CONCURRENCY workloads=[$WORKLOADS] soak=${SOAK_CHUNKS}x$SOAK_REQUESTS"
snapshot "start"

for rep in $(seq 1 "$REPEATS"); do
  for wl in $WORKLOADS; do
    # alternate which arm goes first, so neither always runs on a fresher machine
    if (( rep % 2 )); then arms="R0 R1"; else arms="R1 R0"; fi
    for arm in $arms; do
      job=$(echo "p-$arm-$wl-$rep" | tr 'A-Z' 'a-z')
      restart statefulset/sim && restart deploy/router-epp || { log "restart failed before $job"; fail_check; continue; }
      run_job "$job" "$arm" "$wl" "$REQUESTS" "$rep" || fail_check
    done
  done
  snapshot "after repeat $rep"
done

log "soak: R1 shift, $SOAK_CHUNKS chunks of $SOAK_REQUESTS"
restart statefulset/sim && restart deploy/router-epp || log "restart failed before soak"
for c in $(seq 1 "$SOAK_CHUNKS"); do
  run_job "soak-$c" R1 shift "$SOAK_REQUESTS" "$((100 + c))" || fail_check
  snapshot "after soak chunk $c"
done

kubectl -n $NS logs deploy/router-epp -c epp --since=24h 2>&1 | grep -E '"severity_text":"(ERROR|WARN)' > "$OUT/epp-warnings.log"
log "finished"
