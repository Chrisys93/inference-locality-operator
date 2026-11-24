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

func TestParseAndDiffBuckets(t *testing.T) {
	text := `# HELP ilo_bucket_requests_total Completed requests per prefix bucket.
# TYPE ilo_bucket_requests_total counter
ilo_bucket_requests_total{bucket="3"} 10
ilo_bucket_requests_total{bucket="7"} 4
ilo_bucket_owner_served_total{bucket="3"} 9
ilo_bucket_prompt_tokens_total{bucket="3"} 1000
ilo_bucket_cached_tokens_total{bucket="3"} 800
ilo_bucket_other_total{bucket="3"} 99
go_goroutines 12
`
	prev, err := ParseBuckets(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	if prev[3] != (BucketCounts{Requests: 10, OwnerServed: 9, PromptTokens: 1000, CachedTokens: 800}) || prev[7].Requests != 4 {
		t.Fatalf("parsed %+v", prev)
	}
	cur := map[int]BucketCounts{
		3: {Requests: 25, OwnerServed: 24, PromptTokens: 2500, CachedTokens: 2000},
		7: {Requests: 4},
		9: {Requests: 2, PromptTokens: 200},
	}
	d := DiffBuckets(prev, cur)
	if d[3] != (BucketCounts{Requests: 15, OwnerServed: 15, PromptTokens: 1500, CachedTokens: 1200}) {
		t.Fatalf("bucket 3: %+v", d[3])
	}
	if _, ok := d[7]; ok {
		t.Fatal("idle bucket reported")
	}
	if d[9].Requests != 2 {
		t.Fatalf("new bucket: %+v", d[9])
	}
	// counters reset (EPP restarted)
	if r := DiffBuckets(cur, map[int]BucketCounts{3: {Requests: 5}}); r[3].Requests != 5 {
		t.Fatalf("after reset: %+v", r[3])
	}
}
