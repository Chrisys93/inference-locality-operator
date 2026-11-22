"""Summarise a simulator-baseline results directory (output of run-baseline.sh).

Per phase: requests, errors, share of requests on the busiest pod, prompt tokens served from cache (from the
responses' usage), the same from the pods' prefix-cache counters, and how many pods each document went to.
Latencies are printed for completeness only: on the simulator they follow from configured values.

Usage: python3 -I analyze.py <results-dir>   (writes summary.md and summary.json there)
"""

import collections
import json
import pathlib
import statistics
import sys


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def phase(path):
    recs = [json.loads(line) for line in path.open() if line.startswith("{")]
    start = next((r for r in recs if r["type"] == "start"), {})
    reqs = [r for r in recs if r["type"] == "req"]
    ok = [r for r in reqs if r.get("status") == 200]
    pods = collections.Counter(r.get("pod") for r in ok)
    doc_pods = collections.defaultdict(set)
    for r in ok:
        doc_pods[r["doc"]].add(r.get("pod"))
    prompt = sum(r.get("prompt_tokens") or 0 for r in ok)
    cached = sum(r.get("cached_tokens") or 0 for r in ok)
    # counters: last scrape minus first scrape, per pod address
    mets = [r for r in recs if r["type"] == "metrics"]
    first, last = {}, {}
    for m in mets:
        first.setdefault(m["ip"], m["values"])
        last[m["ip"]] = m["values"]
    hits = sum(last[ip].get("vllm:prefix_cache_hits_total", 0) - first[ip].get("vllm:prefix_cache_hits_total", 0)
               for ip in last)
    queries = sum(last[ip].get("vllm:prefix_cache_queries_total", 0)
                  - first[ip].get("vllm:prefix_cache_queries_total", 0) for ip in last)
    lat = [r["latency"] for r in ok]
    t0 = min((r["t0"] for r in reqs), default=0)
    t1 = max((r["t0"] + r["latency"] for r in reqs), default=0)
    return {
        "phase": path.stem,
        "arm": start.get("arm"),
        "workload": start.get("workload"),
        "seed": start.get("seed"),
        "requests": len(reqs),
        "errors": len(reqs) - len(ok),
        "busiest_pod_share": round(max(pods.values()) / len(ok), 3) if ok else None,
        "cached_share_usage": round(cached / prompt, 4) if prompt else None,
        "cached_share_counters": round(hits / queries, 4) if queries else None,
        "mean_pods_per_doc": round(statistics.mean(len(v) for v in doc_pods.values()), 2) if doc_pods else None,
        "sim_latency_p50": round(pct(lat, 0.5), 3) if lat else None,
        "sim_latency_p95": round(pct(lat, 0.95), 3) if lat else None,
        "duration_s": round(t1 - t0, 1),
    }


def main():
    out = pathlib.Path(sys.argv[1])
    rows = [phase(p) for p in sorted(out.glob("*.jsonl")) if not p.name.endswith(".failed.jsonl")]
    (out / "summary.json").write_text(json.dumps(rows, indent=1))

    cols = ["phase", "arm", "workload", "requests", "errors", "busiest_pod_share", "cached_share_usage",
            "cached_share_counters", "mean_pods_per_doc", "sim_latency_p50", "sim_latency_p95", "duration_s"]
    lines = ["# Simulator baseline summary", "",
             "Simulator latencies follow from configured values; they are listed for completeness, not as results.",
             "", "## Per phase", "", "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(r.get(c)) for c in cols) + " |" for r in rows]

    lines += ["", "## Comparison phases: mean over repeats", "",
              "| arm | workload | phases | errors | busiest_pod_share | cached_share_usage | cached_share_counters"
              " | mean_pods_per_doc |", "|---|---|---|---|---|---|---|---|"]
    groups = collections.defaultdict(list)
    for r in rows:
        if r["phase"].startswith("p-"):
            groups[(r["arm"], r["workload"])].append(r)

    def mean(rs, key):
        vals = [x[key] for x in rs if x[key] is not None]
        return round(statistics.mean(vals), 3) if vals else None

    for (arm, wl), rs in sorted(groups.items()):
        lines.append(f"| {arm} | {wl} | {len(rs)} | {sum(x['errors'] for x in rs)} | {mean(rs, 'busiest_pod_share')}"
                     f" | {mean(rs, 'cached_share_usage')} | {mean(rs, 'cached_share_counters')}"
                     f" | {mean(rs, 'mean_pods_per_doc')} |")
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
