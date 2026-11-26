"""Closed-loop load generator for the simulator baseline (standard library only).

Each request is a chat completion whose prompt is a shared preamble, one of D "documents" and a unique question, the
shape of a RAG prompt. Which document is chosen depends on the workload:

  uniform    every document equally likely
  zipf       Zipf popularity (exponent ZIPF_S) over a fixed ranking
  shift      Zipf, but the ranking rotates every quarter of the run (popular set moves)

Arrivals: closed loop by default (CONCURRENCY workers, each sending its next request when the last one returns); with
RATE > 0, open loop: requests are sent at Poisson arrival times of RATE per second, whatever the service's speed (up to
MAX_INFLIGHT outstanding), as independent users arrive.

Writes one JSON line per request to stdout, plus one "metrics" line per pod before and after the run (read from the
pods' /metrics through the headless Service). Configuration through environment variables, see below.
"""

import json
import os
import random
import socket
import threading
import time
import urllib.request

TARGET = os.environ["TARGET"]  # base URL, e.g. http://sim-plain:8000
ARM = os.environ.get("ARM", "")
WORKLOAD = os.environ.get("WORKLOAD", "zipf")
REQUESTS = int(os.environ.get("REQUESTS", "1500"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "6"))
DOCS = int(os.environ.get("DOCS", "32"))
DOC_WORDS = int(os.environ.get("DOC_WORDS", "300"))
HOT_OFFSET = int(os.environ.get("HOT_OFFSET") or "0")  # zipf: document (rank + HOT_OFFSET) % DOCS is the rank-th most popular
ZIPF_S = float(os.environ.get("ZIPF_S", "1.2"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS") or "16")  # unset or empty: 16
MODEL = os.environ.get("MODEL") or "sim-model"  # served model name (vLLM: the tier's name)
SEED = int(os.environ.get("SEED", "1"))
METRICS_HOST = os.environ.get("METRICS_HOST", "sim-direct")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
TIMEOUT = float(os.environ.get("TIMEOUT", "120"))
RATE = float(os.environ.get("RATE") or "0")  # requests per second, open loop; 0 = closed loop
MAX_INFLIGHT = int(os.environ.get("MAX_INFLIGHT") or "64")

METRIC_NAMES = (
    "vllm:prefix_cache_hits_total",
    "vllm:prefix_cache_queries_total",
    "vllm:num_requests_running",
    "vllm:num_requests_waiting",
    "vllm:kv_cache_usage_perc",
)

out_lock = threading.Lock()


def emit(obj):
    with out_lock:
        print(json.dumps(obj), flush=True)


def words(rng, n):
    vocab = [f"w{i}" for i in range(5000)]
    return " ".join(rng.choice(vocab) for _ in range(n))


def build_docs():
    rng = random.Random(1000)  # same documents in every run
    preamble = "You are a code documentation assistant. Answer from the context only. " + words(rng, 60)
    docs = [f"[doc {d}] " + words(rng, DOC_WORDS) for d in range(DOCS)]
    return preamble, docs


def zipf_weights(n, s):
    return [1.0 / (r + 1) ** s for r in range(n)]


def pick_doc(rng, i):
    if WORKLOAD == "uniform":
        return rng.randrange(DOCS)
    rank = rng.choices(range(DOCS), weights=ZIPF_WEIGHTS)[0]
    if WORKLOAD == "shift":
        quarter = min(3, i * 4 // REQUESTS)
        return (rank + quarter * (DOCS // 4)) % DOCS
    return (rank + HOT_OFFSET) % DOCS


def scrape():
    try:
        addrs = sorted({a[4][0] for a in socket.getaddrinfo(METRICS_HOST, METRICS_PORT, socket.AF_INET)})
    except OSError as e:
        emit({"type": "metrics_error", "error": str(e)})
        return
    for ip in addrs:
        try:
            body = urllib.request.urlopen(f"http://{ip}:{METRICS_PORT}/metrics", timeout=10).read().decode()
        except OSError as e:
            emit({"type": "metrics_error", "ip": ip, "error": str(e)})
            continue
        vals = {}
        for line in body.splitlines():
            if line.startswith("#"):
                continue
            for name in METRIC_NAMES:
                if line.startswith(name):
                    try:
                        vals[name] = vals.get(name, 0.0) + float(line.rsplit(" ", 1)[1])
                    except ValueError:
                        pass
        emit({"type": "metrics", "ip": ip, "t": time.time(), "values": vals})


ZIPF_WEIGHTS = zipf_weights(DOCS, ZIPF_S)
PREAMBLE, DOC_TEXT = build_docs()
counter = iter(range(REQUESTS))
counter_lock = threading.Lock()


def worker(wid):
    rng = random.Random(SEED * 1000 + wid)
    while True:
        with counter_lock:
            i = next(counter, None)
        if i is None:
            return
        send(rng, i)


def open_loop():
    """Poisson arrivals at RATE per second; each request in its own thread, at most MAX_INFLIGHT at once."""
    rng = random.Random(SEED * 1000)
    slots = threading.BoundedSemaphore(MAX_INFLIGHT)
    threads, start, due = [], time.time(), 0.0
    for i in range(REQUESTS):
        due += rng.expovariate(RATE)
        delay = start + due - time.time()
        if delay > 0:
            time.sleep(delay)
        slots.acquire()
        req_rng = random.Random(rng.random())

        def run(i=i, req_rng=req_rng, sched=start + due):
            try:
                send(req_rng, i, sched)
            finally:
                slots.release()
        th = threading.Thread(target=run)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()


def send(rng, i, sched=None):
    """One request: pick a document, send the prompt, emit its record."""
    doc = pick_doc(rng, i)
    prompt = f"{PREAMBLE}\n\nContext:\n{DOC_TEXT[doc]}\n\nQuestion {i}: " + words(rng, 15)
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
    }).encode()
    req = urllib.request.Request(f"{TARGET}/v1/chat/completions", data=body,
                                 headers={"Content-Type": "application/json"})
    t0 = time.time()
    rec = {"type": "req", "i": i, "doc": doc, "t0": t0}
    if sched is not None:
        rec["late"] = round(t0 - sched, 3)  # open loop: how far behind its arrival time it was sent
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            data = json.loads(resp.read())
            rec["status"] = resp.status
            rec["pod"] = resp.headers.get("x-inference-pod")
            usage = data.get("usage") or {}
            rec["prompt_tokens"] = usage.get("prompt_tokens")
            rec["cached_tokens"] = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
    except urllib.error.HTTPError as e:
        rec["status"] = e.code
        rec["error"] = e.read()[:200].decode(errors="replace")
    except Exception as e:  # noqa: BLE001 - recorded, not fatal
        rec["status"] = 0
        rec["error"] = repr(e)[:200]
    rec["latency"] = time.time() - t0
    emit(rec)


def main():
    emit({"type": "start", "arm": ARM, "workload": WORKLOAD, "hot_offset": HOT_OFFSET, "requests": REQUESTS,
          "concurrency": CONCURRENCY, "rate": RATE,
          "docs": DOCS, "doc_words": DOC_WORDS, "zipf_s": ZIPF_S, "seed": SEED, "target": TARGET, "t": time.time()})
    scrape()
    if RATE > 0:
        open_loop()
    else:
        threads = [threading.Thread(target=worker, args=(w,)) for w in range(CONCURRENCY)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    scrape()
    emit({"type": "end", "t": time.time()})


if __name__ == "__main__":
    main()
