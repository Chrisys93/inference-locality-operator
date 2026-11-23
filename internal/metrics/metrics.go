// Package metrics reads the vLLM metrics the controller needs from a model server's Prometheus endpoint and turns
// two successive readings into per-interval figures.
package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// vLLM metric names (the simulator uses the same).
const (
	QueueTimeSum     = "vllm:request_queue_time_seconds_sum"
	InferenceTimeSum = "vllm:request_inference_time_seconds_sum"
	InferenceCount   = "vllm:request_inference_time_seconds_count"
	KVCacheUsage     = "vllm:kv_cache_usage_perc"
	RequestsWaiting  = "vllm:num_requests_waiting"
	PrefixHits       = "vllm:prefix_cache_hits_total"
	PrefixQueries    = "vllm:prefix_cache_queries_total"
)

var wanted = []string{QueueTimeSum, InferenceTimeSum, InferenceCount, KVCacheUsage, RequestsWaiting, PrefixHits,
	PrefixQueries}

// Sample is one reading of a replica: each wanted series summed over its labels.
type Sample struct {
	Time   time.Time
	Values map[string]float64
}

// Parse reads Prometheus text format and sums the wanted series over their labels.
func Parse(r io.Reader, at time.Time) (Sample, error) {
	s := Sample{Time: at, Values: map[string]float64{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		for _, w := range wanted {
			if name != w {
				continue
			}
			// value is the first field after the labels; an optional timestamp may follow
			rest := line[len(name):]
			if strings.HasPrefix(rest, "{") {
				end := strings.LastIndex(rest, "}")
				if end < 0 {
					return s, fmt.Errorf("malformed line %q", line)
				}
				rest = rest[end+1:]
			}
			fields := strings.Fields(rest)
			if len(fields) == 0 {
				return s, fmt.Errorf("no value in %q", line)
			}
			v, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return s, fmt.Errorf("value in %q: %w", line, err)
			}
			s.Values[name] += v
		}
	}
	return s, sc.Err()
}

// Interval is what happened on a replica between two samples.
type Interval struct {
	Requests       int64
	MeanQueueSec   float64 // queueing delay per completed request
	MeanServiceSec float64 // inference time per completed request
	KVCacheUsage   float64 // at the later sample, 0..1
	Waiting        float64 // at the later sample
	PrefixHitRate  float64 // hits / queries in the interval, 0 if no queries
}

// Diff turns two samples into per-interval figures. Counters that went down (a restarted server) are treated as
// starting from zero. Mean queueing delay is divided by the inference count, not the queue histogram's own count,
// because a server may record queue time only for requests that waited.
func Diff(prev, cur Sample) Interval {
	d := func(name string) float64 {
		c, p := cur.Values[name], prev.Values[name]
		if c < p {
			return c
		}
		return c - p
	}
	iv := Interval{
		KVCacheUsage: cur.Values[KVCacheUsage],
		Waiting:      cur.Values[RequestsWaiting],
	}
	n := d(InferenceCount)
	iv.Requests = int64(n)
	if n > 0 {
		iv.MeanQueueSec = d(QueueTimeSum) / n
		iv.MeanServiceSec = d(InferenceTimeSum) / n
	}
	if q := d(PrefixQueries); q > 0 {
		iv.PrefixHitRate = d(PrefixHits) / q
	}
	return iv
}

// Scraper reads one replica's metrics.
type Scraper interface {
	Scrape(ctx context.Context, url string) (Sample, error)
}

// HTTPScraper reads metrics over HTTP.
type HTTPScraper struct {
	Client *http.Client
}

// Scrape implements Scraper.
func (h HTTPScraper) Scrape(ctx context.Context, url string) (Sample, error) {
	c := h.Client
	if c == nil {
		c = &http.Client{Timeout: 3 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Sample{}, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return Sample{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Sample{}, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return Parse(resp.Body, time.Now())
}
