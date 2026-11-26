package bucketaffinity

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// Warming moves a bucket's data with the bucket, as ReStorEdge moves a bucket together with its data: the scorer keeps
// the last few prompts of each bucket, and when the map gives a bucket a new owner it replays them to that owner (one
// output token each, so the cost is their prefill) while it keeps routing the bucket's requests to the old owner. Once
// the replay is done the bucket's requests go to the new owner, whose prefix cache now holds the bucket's prefixes.
// Only that bucket's routing changes, and only after its data is in place. Prompts stay in the router's memory.

const (
	defaultWarmPrompts = 4
	warmRequestTimeout = 60 * time.Second
	warmTotalTimeout   = 3 * time.Minute
)

// replay is one remembered request, ready to send to a model server.
type replay struct {
	path string
	body []byte
}

// hold keeps a bucket on its previous owner while the new owner is warmed.
type hold struct {
	owner string
	gen   uint64
}

type warmer struct {
	keep   int
	client *http.Client

	mu     sync.Mutex
	recent map[int][]replay  // per bucket, oldest first, at most keep
	addr   map[string]string // endpoint name -> base URL, from the candidates the scheduler offers
	holds  map[int]hold      // buckets being warmed -> the owner that keeps serving them meanwhile
	gen    uint64
	wg     sync.WaitGroup // replays in flight (tests wait on it)
	sent   *prometheus.CounterVec
	failed prometheus.Counter
	took   prometheus.Histogram
}

func newWarmer(keep int, reg prometheus.Registerer) *warmer {
	if keep <= 0 {
		keep = defaultWarmPrompts
	}
	w := &warmer{
		keep:   keep,
		client: &http.Client{Timeout: warmRequestTimeout},
		recent: map[int][]replay{},
		addr:   map[string]string{},
		holds:  map[int]hold{},
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricNamespace, Subsystem: metricSubsystem,
			Name: "warm_requests_total", Help: "Prompts replayed to warm a bucket's new owner, per bucket."},
			[]string{bucketLabel}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricNamespace, Subsystem: metricSubsystem,
			Name: "warm_failures_total", Help: "Replayed prompts that failed (the bucket moves anyway)."}),
		took: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: metricNamespace, Subsystem: metricSubsystem,
			Name: "warm_seconds", Help: "Time from a bucket's move in the map to its requests going to the new owner.",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 40, 80, 160}}),
	}
	if reg != nil {
		for _, c := range []prometheus.Collector{w.sent, w.failed, w.took} {
			_ = reg.Register(c) // a second plugin instance reuses the first one's series
		}
	}
	return w
}

// remember keeps the request as one of its bucket's last prompts.
func (w *warmer) remember(bucket int, req *fwksched.InferenceRequest) {
	r, ok := replayOf(req)
	if !ok {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	list := append(w.recent[bucket], r)
	if len(list) > w.keep {
		list = list[len(list)-w.keep:]
	}
	w.recent[bucket] = list
}

// replayOf builds the request that re-sends the prompt with one output token. Message contents are sent as their plain
// text, the same text the bucket key is taken from.
func replayOf(req *fwksched.InferenceRequest) (replay, bool) {
	if req == nil || req.Body == nil {
		return replay{}, false
	}
	switch {
	case req.Body.ChatCompletions != nil:
		type msg struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}
		msgs := make([]msg, 0, len(req.Body.ChatCompletions.Messages))
		for _, m := range req.Body.ChatCompletions.Messages {
			msgs = append(msgs, msg{Role: m.Role, Content: m.Content.PlainText()})
		}
		b, err := json.Marshal(map[string]any{"model": req.TargetModel, "messages": msgs, "max_tokens": 1})
		return replay{path: "/v1/chat/completions", body: b}, err == nil
	case req.Body.Completions != nil:
		b, err := json.Marshal(map[string]any{"model": req.TargetModel,
			"prompt": req.Body.Completions.Prompt.PlainText(), "max_tokens": 1})
		return replay{path: "/v1/completions", body: b}, err == nil
	}
	return replay{}, false
}

// noteEndpoints records where each candidate endpoint can be reached.
func (w *warmer) noteEndpoints(endpoints []fwksched.Endpoint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range endpoints {
		if md := e.GetMetadata(); md != nil && md.Name != "" && md.Address != "" && md.Port != "" {
			w.addr[md.Name] = "http://" + net.JoinHostPort(md.Address, md.Port)
		}
	}
}

// route returns the replica that serves the bucket now: its previous owner while the new owner is being warmed, if
// that replica is still a candidate; otherwise the map's owner.
func (w *warmer) route(bucket int, owner string, endpoints []fwksched.Endpoint) string {
	w.mu.Lock()
	h, ok := w.holds[bucket]
	w.mu.Unlock()
	if !ok {
		return owner
	}
	if slices.ContainsFunc(endpoints, func(e fwksched.Endpoint) bool {
		return e.GetMetadata() != nil && e.GetMetadata().Name == h.owner
	}) {
		return h.owner
	}
	return owner
}

// moved warms the new owner of every bucket whose owner changed between two maps of the same shape. A replica that
// left (not a candidate any more) cannot serve meanwhile; route falls back to the new owner for it.
func (w *warmer) moved(ctx context.Context, prev, next *buckets.Map) {
	if prev == nil || next == nil || prev.Buckets != next.Buckets || prev.PrefixBytes != next.PrefixBytes ||
		len(prev.Owners) != len(next.Owners) {
		return
	}
	for b := range next.Owners {
		if from, to := prev.Owners[b], next.Owners[b]; from != "" && from != to {
			w.start(ctx, b, from, to)
		}
	}
}

func (w *warmer) start(ctx context.Context, bucket int, from, to string) {
	w.mu.Lock()
	prompts := slices.Clone(w.recent[bucket])
	target := w.addr[to]
	if len(prompts) == 0 || target == "" { // nothing to move, or nowhere known to send it: move at once
		delete(w.holds, bucket)
		w.mu.Unlock()
		return
	}
	w.gen++
	g := w.gen
	w.holds[bucket] = hold{owner: from, gen: g}
	w.mu.Unlock()

	w.wg.Go(func() {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmTotalTimeout)
		defer cancel()
		label := strconv.Itoa(bucket)
		for _, r := range prompts {
			if err := w.send(ctx, target, r); err != nil {
				w.failed.Inc()
				log.FromContext(ctx).V(2).Info("warming", "bucket", bucket, "to", to, "error", err.Error())
				continue
			}
			w.sent.WithLabelValues(label).Inc()
		}
		w.mu.Lock()
		if h, ok := w.holds[bucket]; ok && h.gen == g {
			delete(w.holds, bucket)
		}
		w.mu.Unlock()
		w.took.Observe(time.Since(start).Seconds())
		log.FromContext(ctx).V(2).Info("bucket warmed", "bucket", bucket, "from", from, "to", to,
			"prompts", len(prompts), "seconds", time.Since(start).Seconds())
	})
}

func (w *warmer) send(ctx context.Context, base string, r replay) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+r.path, bytes.NewReader(r.body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // nothing to do on a failed close
	if resp.StatusCode/100 != 2 {
		return &statusError{code: resp.StatusCode}
	}
	return nil
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "status " + strconv.Itoa(e.code) }
