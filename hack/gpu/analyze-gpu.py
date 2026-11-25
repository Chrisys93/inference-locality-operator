#!/usr/bin/env python3
"""Summarise a hack/gpu/run-gpu.sh results directory, per run and per (arm, offset).

vLLM returns no pod header, so the share of the busiest replica comes from each replica's own counters
(<job>.vllm.jsonl minus <job>.vllm-before.jsonl: the replicas stay loaded across runs), and its course over the run from the policy
status recorded every 10 s (requests per replica per 30 s interval). Latency and cached prompt tokens come from the
load generator's records.
    python3 -I hack/gpu/analyze-gpu.py results/gpu-rebalance-001
"""
import collections
import json
import pathlib
import statistics
import sys


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


def load_lines(path):
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()] if path.exists() else []


def intervals(status):
    """Distinct controller intervals (by lastRebalanceTime / replica figures), each as {pod: requests}."""
    seen, out = set(), []
    for s in status:
        reps = s.get("replicas") or []
        key = json.dumps([(r["name"], r["requests"], r["meanQueueMillis"]) for r in reps])
        if key in seen or not any(r["requests"] for r in reps):
            continue
        seen.add(key)
        out.append({"replicas": {r["name"]: r for r in reps}})
    return out


def main(d):
    d = pathlib.Path(d)
    rows = []
    for f in sorted(d.glob("gpu-*.jsonl")):
        if f.name.endswith((".status.jsonl", ".failed.jsonl", ".vllm.jsonl")):
            continue
        job = f.name[: -len(".jsonl")]
        reqs = [r for r in load_lines(f) if r.get("type") == "req"]
        ok = [r for r in reqs if r.get("status") == 200]
        if not reqs:
            continue
        lat = [r["latency"] for r in ok]
        prompt = sum(r.get("prompt_tokens") or 0 for r in ok)
        cached = sum(r.get("cached_tokens") or 0 for r in ok)
        span = max(r["t0"] + r["latency"] for r in reqs) - min(r["t0"] for r in reqs)
        before = {v["pod"]: v for v in load_lines(d / f"{job}.vllm-before.jsonl")}
        vl = [{k: (x - before[v["pod"]].get(k, 0) if isinstance(x, (int, float)) and v["pod"] in before else x)
               for k, x in v.items()} for v in load_lines(d / f"{job}.vllm.jsonl")]  # this run's share only
        total = sum(v["requests"] for v in vl) or 1
        moves_log = (d / f"{job}.moves.log")
        lines = moves_log.read_text().splitlines() if moves_log.exists() else []
        ivs = intervals(load_lines(d / f"{job}.status.jsonl"))
        shares, ratios, hot = [], [], 0
        for iv in ivs:
            reps = iv["replicas"].values()
            n = sum(r["requests"] for r in reps)
            if n:
                shares.append(max(r["requests"] for r in reps) / n)
            for r in reps:
                if r["requests"] >= 20 and r["meanServiceMillis"]:
                    ratios.append(r["meanQueueMillis"] / r["meanServiceMillis"])
            hot += any(r.get("overloaded") for r in reps)
        last = shares[len(shares) // 2:] or shares
        rows.append({
            "run": job,
            "requests": len(reqs),
            "errors": len(reqs) - len(ok),
            "busiest_share": round(max(v["requests"] for v in vl) / total, 3) if vl else None,
            "busiest_share_2nd_half": round(statistics.mean(last), 3) if last else None,
            "cached_share": round(cached / prompt, 3) if prompt else None,
            "lat_p50": round(pct(lat, 0.5), 2),
            "lat_p95": round(pct(lat, 0.95), 2),
            "req_per_s": round(len(ok) / span, 2) if span else None,
            "moves": sum("bucket moved" in x for x in lines),
            "hot_intervals": hot,
            "max_queue_ratio": round(max(ratios), 2) if ratios else None,
            "preemptions": int(sum(v.get("preemptions", 0) for v in vl)),
            "queue_s_per_req": round(sum(v["queue_s"] for v in vl) / total, 2) if vl else None,
        })

    arms = collections.defaultdict(list)
    for r in rows:
        arms["-".join(r["run"].split("-")[1:-1])].append(r)  # arm and offset

    keys = ["busiest_share", "busiest_share_2nd_half", "cached_share", "lat_p50", "lat_p95", "req_per_s", "moves",
            "hot_intervals", "queue_s_per_req"]
    out = ["# Routing and rebalancing on the GPU (two vLLM replicas, lightest tier)", "",
           "## Per run", "", "| " + " | ".join(rows[0]) + " |", "|" + "---|" * len(rows[0])]
    out += ["| " + " | ".join(str(v) for v in r.values()) + " |" for r in rows]
    out += ["", "## Per arm and offset: mean over runs (min-max)", "",
            "| arm, offset | runs | " + " | ".join(keys) + " |", "|---|---|" + "---|" * len(keys)]
    for arm, rs in sorted(arms.items()):
        cells = []
        for k in keys:
            vals = [r[k] for r in rs if r[k] is not None]
            cells.append(f"{round(statistics.mean(vals), 3)} ({min(vals)}-{max(vals)})" if vals else "-")
        out.append(f"| {arm} | {len(rs)} | " + " | ".join(cells) + " |")
    (d / "summary.md").write_text("\n".join(out) + "\n")
    (d / "summary.json").write_text(json.dumps({"runs": rows}, indent=1))
    print("\n".join(out))


if __name__ == "__main__":
    main(sys.argv[1])
