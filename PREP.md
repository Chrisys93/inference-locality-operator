# Preparation

What has to be in place before each step, and the order of work. Steps are marked done only once they have run.

## Machine

- Single-node minikube (Kubernetes 1.37) in Docker Desktop on WSL2, 12 GB VM, one 12 GB NVIDIA GPU, which pods get
  through `NVIDIA_VISIBLE_DEVICES=all` (no device-plugin request needed).
- Every arm of a comparison runs under the same machine conditions: same memory limit, same desktop load, nothing
  closed or resized for one arm.
- Nothing is changed on the cluster while a model loads.

## Tools (done 2026-11-19)

Installed in the user's home directory, downloads checked against the published checksums:

| Tool | Version | Where |
|---|---|---|
| Go | 1.27.4 | `~/.local/go` |
| kubebuilder | v4.16.0 | `~/.local/bin` |
| controller-gen, envtest binaries | from the Makefile | `./bin` |

```sh
export PATH=$HOME/.local/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH
make manifests generate build test
```

## Steps

1. **Scaffold** (done 2026-11-19): `kubebuilder init` and `create api` for `PrefixAffinityPolicy`; builds, envtest
   passes.
2. **Baseline on simulators:** Gateway API Inference Extension CRDs, two `llm-d-inference-sim` replicas, the
   standalone llm-d router chart with the EPP sized for this node (the chart asks for 8 CPU / 8 GiB by default;
   about 1 CPU / 1 GiB here), prefix-cache and queue scorers. Requests sent through the router's Envoy; routing
   decisions and per-replica metrics logged.
3. **Controller:** the `PrefixAffinityPolicy` types, metrics scraping, bucket assignment and rebalancing, the
   ConfigMap, status; unit tests with envtest and fake metrics endpoints.
4. **Scorer:** fork of the EPP at v1.0.0 with `bucket-affinity-scorer`; image built locally and loaded into minikube.
5. **Measurement on GPU:** two vLLM replicas sharing the GPU; arms R0-R3 of `ARCHITECTURE.md` with the
   code-doc-assistant evaluation set and synthetic skewed workloads.
6. **`CachePolicy`.**

## Relation to code-doc-assistant

[code-doc-assistant](https://github.com/Chrisys93/code-doc-assistant) is the earlier work this builds on: its Helm
chart, vLLM settings for the 12 GB card (measured there), evaluation set and harness provide the workload. Its
planned stock-llm-d measurement is the R1 arm here. Nothing in this repository depends on its code at build time.
