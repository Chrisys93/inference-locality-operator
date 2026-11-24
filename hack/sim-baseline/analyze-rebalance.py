#!/usr/bin/env python3
"""Summarise a run-rebalance.sh results directory: per run and per minute, the busiest replica's share of requests,
the cached share of prompt tokens, simulated latency, the moves made and the replicas' queue-to-service ratio.
Simulator latencies follow from configured values: they show queueing relief, not performance.
    python3 -I hack/sim-baseline/analyze-rebalance.py results/rebalance-001
"""
import collections
import json
import pathlib
import statistics
import sys


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


def summarise(reqs):
    ok = [r for r in reqs if r.get("status") == 200]
    pods = collections.Counter(r["pod"] for r in ok)
    prompt = sum(r.get("prompt_tokens", 0) for r in ok)
    cached = sum(r.get("cached_tokens", 0) for r in ok)
    lat = [r["latency"] for r in ok]
    span = max(r["t0"] + r["latency"] for r in reqs) - min(r["t0"] for r in reqs) if reqs else 0
    return {
        "requests": len(reqs),
        "errors": len(reqs) - len(ok),
        "busiest_share": round(max(pods.values()) / len(ok), 3) if ok else None,
        "cached_share": round(cached / prompt, 3) if prompt else None,
        "lat_p50": round(pct(lat, 0.5), 3),
        "lat_p95": round(pct(lat, 0.95), 3),
        "req_per_s": round(len(ok) / span, 1) if span else None,
    }


def main(d):
    d = pathlib.Path(d)
    rows, minutes = [], []
    for f in sorted(d.glob("rb-*.jsonl")):
        if f.name.endswith((".status.jsonl", ".failed.jsonl")):
            continue
        job = f.name[: -len(".jsonl")]
        reqs = [json.loads(line) for line in f.read_text().splitlines() if '"type": "req"' in line]
        if not reqs:
            continue
        moves_log = d / f"{job}.moves.log"
        lines = moves_log.read_text().splitlines() if moves_log.exists() else []
        moved = sum("bucket moved" in x for x in lines)
        stuck = sum("no bucket can move" in x for x in lines)
        status = [json.loads(x) for x in (d / f"{job}.status.jsonl").read_text().splitlines()] \
            if (d / f"{job}.status.jsonl").exists() else []
        ratios = []
        for s in status:
            for r in s.get("replicas", []):
                if r.get("requests", 0) >= 20 and r.get("meanServiceMillis"):
                    ratios.append(r["meanQueueMillis"] / r["meanServiceMillis"])
        row = {"run": job, **summarise(reqs), "moves": moved, "stuck_intervals": stuck,
               "max_queue_ratio": round(max(ratios), 2) if ratios else None}
        rows.append(row)
        t0 = min(r["t0"] for r in reqs)
        by_min = collections.defaultdict(list)
        for r in reqs:
            by_min[int((r["t0"] - t0) // 60)].append(r)
        for m in sorted(by_min):
            minutes.append({"run": job, "minute": m, **summarise(by_min[m])})

    arms = collections.defaultdict(list)
    for r in rows:
        arms["-".join(r["run"].split("-")[1:-1])].append(r)  # arm and offset

    out = ["# Rebalancing comparison (simulators)", "",
           "Simulator latencies follow from configured values; they show queueing relief, not performance.", "",
           "## Per run", "", "| " + " | ".join(rows[0]) + " |", "|" + "---|" * len(rows[0])]
    out += ["| " + " | ".join(str(v) for v in r.values()) + " |" for r in rows]
    out += ["", "## Per arm: mean over runs", "",
            "| arm, offset | runs | busiest_share | cached_share | lat_p95 | req_per_s | moves |",
            "|---|---|---|---|---|---|---|"]
    for arm, rs in sorted(arms.items()):
        def mean(k):
            return round(statistics.mean(r[k] for r in rs), 3)
        out.append(f"| {arm} | {len(rs)} | {mean('busiest_share')} | {mean('cached_share')} | {mean('lat_p95')} | "
                   f"{mean('req_per_s')} | {mean('moves')} |")
    out += ["", "## Per minute", "", "| run | minute | requests | busiest_share | cached_share | lat_p95 |",
            "|---|---|---|---|---|---|"]
    out += [f"| {m['run']} | {m['minute']} | {m['requests']} | {m['busiest_share']} | {m['cached_share']} | "
            f"{m['lat_p95']} |" for m in minutes]
    (d / "summary.md").write_text("\n".join(out) + "\n")
    (d / "summary.json").write_text(json.dumps({"runs": rows, "minutes": minutes}, indent=1))
    print("\n".join(out[: 12 + len(rows) + len(arms)]))


if __name__ == "__main__":
    main(sys.argv[1])
