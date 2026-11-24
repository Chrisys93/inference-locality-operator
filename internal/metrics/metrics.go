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
	var s Sample
	err := h.get(ctx, url, "", func(r io.Reader) error {
		var perr error
		s, perr = Parse(r, time.Now())
		return perr
	})
	return s, err
}

// ScrapeBuckets implements BucketScraper.
func (h HTTPScraper) ScrapeBuckets(ctx context.Context, url, bearer string) (map[int]BucketCounts, error) {
	var b map[int]BucketCounts
	err := h.get(ctx, url, bearer, func(r io.Reader) error {
		var perr error
		b, perr = ParseBuckets(r)
		return perr
	})
	return b, err
}

func (h HTTPScraper) get(ctx context.Context, url, bearer string, read func(io.Reader) error) error {
	c := h.Client
	if c == nil {
		c = &http.Client{Timeout: 3 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return read(resp.Body)
}

// Per-bucket counters exported by bucket-affinity-scorer, labelled bucket="<index>".
const (
	BucketRequests     = "ilo_bucket_requests_total"
	BucketOwnerServed  = "ilo_bucket_owner_served_total"
	BucketPromptTokens = "ilo_bucket_prompt_tokens_total"
	BucketCachedTokens = "ilo_bucket_cached_tokens_total"
)

// BucketCounts are one bucket's counters: cumulative as read, or per interval after DiffBuckets.
type BucketCounts struct {
	Requests     float64
	OwnerServed  float64
	PromptTokens float64
	CachedTokens float64
}

// BucketScraper reads the scorer's per-bucket counters; bearer, when not empty, is sent as a bearer token.
type BucketScraper interface {
	ScrapeBuckets(ctx context.Context, url, bearer string) (map[int]BucketCounts, error)
}

// ParseBuckets reads the per-bucket counters from Prometheus text format.
func ParseBuckets(r io.Reader) (map[int]BucketCounts, error) {
	out := map[int]BucketCounts{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "ilo_bucket_") {
			continue
		}
		open, end := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
		if open < 0 || end < open {
			continue
		}
		name := line[:open]
		_, label, ok := strings.Cut(line[open+1:end], `bucket="`)
		if !ok {
			continue
		}
		label, _, _ = strings.Cut(label, `"`)
		b, err := strconv.Atoi(label)
		if err != nil || b < 0 {
			return nil, fmt.Errorf("bucket label in %q", line)
		}
		fields := strings.Fields(line[end+1:])
		if len(fields) == 0 {
			return nil, fmt.Errorf("no value in %q", line)
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("value in %q: %w", line, err)
		}
		c := out[b]
		switch name {
		case BucketRequests:
			c.Requests = v
		case BucketOwnerServed:
			c.OwnerServed = v
		case BucketPromptTokens:
			c.PromptTokens = v
		case BucketCachedTokens:
			c.CachedTokens = v
		default:
			continue
		}
		out[b] = c
	}
	return out, sc.Err()
}

// DiffBuckets returns each bucket's counts between two readings. A bucket whose request count went down (the EPP
// restarted) counts from zero.
func DiffBuckets(prev, cur map[int]BucketCounts) map[int]BucketCounts {
	out := make(map[int]BucketCounts, len(cur))
	for b, c := range cur {
		p := prev[b]
		if c.Requests < p.Requests {
			p = BucketCounts{}
		}
		d := BucketCounts{
			Requests:     c.Requests - p.Requests,
			OwnerServed:  c.OwnerServed - p.OwnerServed,
			PromptTokens: c.PromptTokens - p.PromptTokens,
			CachedTokens: c.CachedTokens - p.CachedTokens,
		}
		if d.Requests > 0 {
			out[b] = d
		}
	}
	return out
}
