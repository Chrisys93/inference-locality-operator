package metrics

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// as served by llm-d-inference-sim v1.2.0 (names as in vLLM)
const text = `# HELP vllm:kv_cache_usage_perc KV-cache usage.
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="sim-model"} 0.25
vllm:num_requests_waiting{model_name="sim-model"} 3
vllm:prefix_cache_hits_total{model_name="sim-model"} 900
vllm:prefix_cache_queries_total{model_name="sim-model"} 1000
vllm:request_inference_time_seconds_bucket{model_name="sim-model",le="0.3"} 10
vllm:request_inference_time_seconds_sum{model_name="sim-model"} 20
vllm:request_inference_time_seconds_count{model_name="sim-model"} 100
vllm:request_queue_time_seconds_sum{model_name="sim-model"} 5
vllm:request_queue_time_seconds_count{model_name="sim-model"} 60
vllm:request_queue_time_seconds_sum_other{a="b"} 99
`

func TestParse(t *testing.T) {
	s, err := Parse(strings.NewReader(text), time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{KVCacheUsage: 0.25, RequestsWaiting: 3, PrefixHits: 900, PrefixQueries: 1000,
		InferenceTimeSum: 20, InferenceCount: 100, QueueTimeSum: 5}
	for k, v := range want {
		if s.Values[k] != v {
			t.Errorf("%s = %v, want %v", k, s.Values[k], v)
		}
	}
	if len(s.Values) != len(want) {
		t.Errorf("extra series: %v", s.Values)
	}
}

func TestParseSumsLabels(t *testing.T) {
	s, err := Parse(strings.NewReader("vllm:num_requests_waiting{model_name=\"a\"} 1\nvllm:num_requests_waiting{model_name=\"b\"} 2 1700000000\n"), time.Now())
	if err != nil || s.Values[RequestsWaiting] != 3 {
		t.Fatalf("%v %v", err, s.Values)
	}
}

func TestDiff(t *testing.T) {
	prev := Sample{Values: map[string]float64{InferenceCount: 100, InferenceTimeSum: 20, QueueTimeSum: 5,
		PrefixHits: 900, PrefixQueries: 1000}}
	cur := Sample{Values: map[string]float64{InferenceCount: 150, InferenceTimeSum: 30, QueueTimeSum: 25,
		PrefixHits: 1350, PrefixQueries: 1500, KVCacheUsage: 0.5}}
	iv := Diff(prev, cur)
	if iv.Requests != 50 || math.Abs(iv.MeanServiceSec-0.2) > 1e-9 || math.Abs(iv.MeanQueueSec-0.4) > 1e-9 ||
		math.Abs(iv.PrefixHitRate-0.9) > 1e-9 || iv.KVCacheUsage != 0.5 {
		t.Fatalf("%+v", iv)
	}
	// restarted server: counters below the previous reading count from zero
	reset := Diff(cur, Sample{Values: map[string]float64{InferenceCount: 10, InferenceTimeSum: 2}})
	if reset.Requests != 10 || math.Abs(reset.MeanServiceSec-0.2) > 1e-9 {
		t.Fatalf("%+v", reset)
	}
}

func TestHTTPScraper(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, text)
	}))
	defer srv.Close()
	s, err := HTTPScraper{}.Scrape(context.Background(), srv.URL+"/metrics")
	if err != nil || s.Values[InferenceCount] != 100 {
		t.Fatalf("%v %v", err, s.Values)
	}
	if _, err := (HTTPScraper{}).Scrape(context.Background(), srv.URL+"/nope"); err == nil {
		t.Fatal("404 accepted")
	}
}
