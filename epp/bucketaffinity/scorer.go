// Package bucketaffinity is the bucket-affinity-scorer EPP plugin: it routes each request towards the replica that
// owns the request's prefix bucket in the map published by the PrefixAffinityPolicy controller, and counts, per
// bucket, the requests, prompt tokens and cached prompt tokens it sees, and how many were served by the owner.
package bucketaffinity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// Type is the plugin type named in the EndpointPickerConfig.
const Type = "bucket-affinity-scorer"

// Metric names: ilo_bucket_<name>{bucket="<n>"}.
const (
	metricNamespace = "ilo"
	metricSubsystem = "bucket"
	bucketLabel     = "bucket"
)

// MapKey is the ConfigMap key holding the map; the controller writes the same key.
const MapKey = "map.json"

var (
	_ fwksched.Scorer             = &Scorer{}
	_ fwkrc.ResponseBodyProcessor = &Scorer{}
)

// Parameters are the plugin's configuration.
type Parameters struct {
	// ConfigMap is the name of the bucket map ConfigMap (<policy>-bucket-map).
	ConfigMap string `json:"configMap"`
	// Namespace of the ConfigMap; defaults to the EPP's own namespace.
	Namespace string `json:"namespace,omitempty"`
	// Warm keeps each bucket's last prompts and, when a bucket moves, replays them to its new owner before routing
	// the bucket there (warm.go).
	Warm bool `json:"warm,omitempty"`
	// WarmPrompts is how many prompts are kept per bucket (default 4).
	WarmPrompts int `json:"warmPrompts,omitempty"`
}

// Scorer scores the owner of the request's bucket 1 and every other replica 0.
type Scorer struct {
	typedName fwkplugin.TypedName
	current   atomic.Pointer[buckets.Map]
	stats     *stats
	warm      *warmer // nil: moves take effect at once
}

// Factory builds the plugin from its parameters and starts watching the map ConfigMap.
func Factory(name string, raw *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var p Parameters
	if raw != nil {
		if err := raw.Decode(&p); err != nil {
			return nil, fmt.Errorf("%s: parameters: %w", Type, err)
		}
	}
	if p.ConfigMap == "" {
		return nil, errors.New(Type + ": parameter configMap is required")
	}
	if p.Namespace == "" {
		p.Namespace = ownNamespace()
	}
	s := New(name, handle.Metrics())
	if p.Warm {
		s.EnableWarming(p.WarmPrompts, handle.Metrics())
	}
	if err := watchMap(handle.Context(), p.Namespace, p.ConfigMap, s.SetMap); err != nil {
		return nil, fmt.Errorf("%s: %w", Type, err)
	}
	return s, nil
}

// New returns a scorer with no map yet; reg, when not nil, receives the per-bucket counters.
func New(name string, reg prometheus.Registerer) *Scorer {
	if name == "" {
		name = Type
	}
	return &Scorer{typedName: fwkplugin.TypedName{Type: Type, Name: name}, stats: newStats(reg)}
}

// TypedName returns the plugin's type and name.
func (s *Scorer) TypedName() fwkplugin.TypedName { return s.typedName }

// Category: the scorer prefers the replica holding the bucket's state.
func (s *Scorer) Category() fwksched.ScorerCategory { return fwksched.Affinity }

// EnableWarming turns on warming of a moved bucket's new owner, keeping keep prompts per bucket.
func (s *Scorer) EnableWarming(keep int, reg prometheus.Registerer) { s.warm = newWarmer(keep, reg) }

// SetMap replaces the map in use; nil clears it (every replica then scores 0). With warming, every bucket the new map
// moves stays on its previous owner until the new owner is warmed.
func (s *Scorer) SetMap(m *buckets.Map) {
	prev := s.current.Swap(m)
	if s.warm != nil {
		s.warm.moved(context.Background(), prev, m)
	}
}

// Map returns the map in use, nil when none has been read.
func (s *Scorer) Map() *buckets.Map { return s.current.Load() }

// bucketOf returns the request's bucket and its owner under m.
func bucketOf(m *buckets.Map, req *fwksched.InferenceRequest) (bucket int, owner string, ok bool) {
	if m == nil || req == nil {
		return 0, "", false
	}
	text, ok := PromptText(req.Body)
	if !ok {
		return 0, "", false
	}
	b := buckets.Key(text, m.PrefixBytes, m.Buckets)
	return b, m.Owners[b], true
}

// Score gives 1 to the endpoint whose pod owns the request's bucket, 0 to the others. Without a map, or for a
// request without a text prompt, every endpoint gets 0, which leaves the decision to the other scorers.
func (s *Scorer) Score(
	_ context.Context, req *fwksched.InferenceRequest, endpoints []fwksched.Endpoint,
) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	b, owner, ok := bucketOf(s.current.Load(), req)
	if ok && s.warm != nil {
		s.warm.noteEndpoints(endpoints)
		owner = s.warm.route(b, owner, endpoints)
	}
	for _, e := range endpoints {
		scores[e] = 0
		if ok && e.GetMetadata() != nil && e.GetMetadata().Name == owner {
			scores[e] = 1
		}
	}
	return scores
}

// ResponseBody counts the request against its bucket once the response is complete.
func (s *Scorer) ResponseBody(
	ctx context.Context, req *fwksched.InferenceRequest, resp *fwkrc.Response, target *fwkdl.EndpointMetadata,
) {
	if resp == nil || !resp.EndOfStream {
		return
	}
	b, owner, ok := bucketOf(s.current.Load(), req)
	if !ok {
		return
	}
	served := ""
	if target != nil {
		served = target.Name
	}
	cached := 0
	if resp.Usage.PromptTokenDetails != nil {
		cached = resp.Usage.PromptTokenDetails.CachedTokens
	}
	s.stats.add(b, served == owner, resp.Usage.PromptTokens, cached)
	if s.warm != nil {
		s.warm.remember(b, req)
	}
	log.FromContext(ctx).V(5).Info("bucket", "bucket", b, "owner", owner, "served", served,
		"promptTokens", resp.Usage.PromptTokens, "cachedTokens", cached)
}

// PromptText returns the text the bucket key is taken from: the prompt of a completions request, or the chat
// messages as "role\ncontent\n" in order. The prompt replayer that warms a new owner must build the same text.
func PromptText(body *fwkrh.InferenceRequestBody) (string, bool) {
	switch {
	case body == nil:
		return "", false
	case body.Completions != nil:
		return body.Completions.Prompt.PlainText(), true
	case body.ChatCompletions != nil:
		var sb strings.Builder
		for _, m := range body.ChatCompletions.Messages {
			sb.WriteString(m.Role)
			sb.WriteByte('\n')
			sb.WriteString(m.Content.PlainText())
			sb.WriteByte('\n')
		}
		return sb.String(), true
	}
	return "", false
}

// stats are the per-bucket counters, exported on the EPP's metrics endpoint.
type stats struct {
	requests, ownerServed, promptTokens, cachedTokens *prometheus.CounterVec
}

func newStats(reg prometheus.Registerer) *stats {
	vec := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricNamespace, Subsystem: metricSubsystem, Name: name, Help: help,
		}, []string{bucketLabel})
	}
	st := &stats{
		requests:     vec("requests_total", "Completed requests per prefix bucket."),
		ownerServed:  vec("owner_served_total", "Completed requests per prefix bucket served by the bucket's owner."),
		promptTokens: vec("prompt_tokens_total", "Prompt tokens per prefix bucket."),
		cachedTokens: vec("cached_tokens_total", "Prompt tokens served from the prefix cache, per prefix bucket."),
	}
	if reg != nil {
		for _, c := range []prometheus.Collector{st.requests, st.ownerServed, st.promptTokens, st.cachedTokens} {
			if err := reg.Register(c); err != nil {
				if _, dup := errors.AsType[prometheus.AlreadyRegisteredError](err); !dup {
					log.Log.Error(err, "registering bucket counters")
				}
			}
		}
	}
	return st
}

func (st *stats) add(bucket int, byOwner bool, prompt, cached int) {
	l := strconv.Itoa(bucket)
	st.requests.WithLabelValues(l).Inc()
	if byOwner {
		st.ownerServed.WithLabelValues(l).Inc()
	}
	st.promptTokens.WithLabelValues(l).Add(float64(prompt))
	st.cachedTokens.WithLabelValues(l).Add(float64(cached))
}

func ownNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(b))
	}
	return "default"
}
