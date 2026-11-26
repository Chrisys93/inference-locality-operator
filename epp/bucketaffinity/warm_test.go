package bucketaffinity

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const testModel = "phi"

// fakeServer stands in for the new owner's model server; release lets its requests complete.
type fakeServer struct {
	*httptest.Server
	mu      sync.Mutex
	bodies  []map[string]any
	release chan struct{}
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{release: make(chan struct{})}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-f.release
		var body map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		body["path"] = r.URL.Path
		f.mu.Lock()
		f.bodies = append(f.bodies, body)
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(f.Close)
	return f
}

func endpointAt(name, url string) fwksched.Endpoint {
	host, port, _ := net.SplitHostPort(url[len("http://"):])
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{Name: name, Address: host, Port: port}, nil, nil)
}

func scoredOwner(s *Scorer, req *fwksched.InferenceRequest, eps []fwksched.Endpoint) string {
	for e, v := range s.Score(context.Background(), req, eps) {
		if v == 1 {
			return e.GetMetadata().Name
		}
	}
	return ""
}

func TestWarmingKeepsAMovedBucketOnItsOldOwnerUntilTheNewOneIsWarm(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New("", reg)
	s.EnableWarming(2, reg)
	srv := newFakeServer(t)
	a, b := endpointAt("a", "http://127.0.0.1:1"), endpointAt("b", srv.URL)
	eps := []fwksched.Endpoint{a, b}

	m, _ := buckets.Assign(nil, []string{"a", "b"}, 16, 25) // 25 bytes: the prompts below share a bucket
	s.SetMap(m)
	req := chat("preamble | document 7 | question")
	req.TargetModel = testModel
	bucket, owner, _ := bucketOf(m, req)
	from := owner
	to := map[string]string{"a": "b", "b": "a"}[from]
	if to == "a" { // the warmed replica must be the fake server: swap names so the bucket moves a -> b
		a, b = endpointAt("a", srv.URL), endpointAt("b", "http://127.0.0.1:1")
		eps = []fwksched.Endpoint{a, b}
	}

	// three requests of the bucket complete; two are kept
	s.Score(context.Background(), req, eps)
	for _, q := range []string{"one", "two", "three"} {
		r := chat("preamble | document 7 | question " + q)
		r.TargetModel = testModel
		s.ResponseBody(context.Background(), r, &fwkrc.Response{EndOfStream: true}, &fwkdl.EndpointMetadata{Name: from})
	}

	moved := *m
	moved.Owners = slices.Clone(m.Owners)
	moved.Owners[bucket] = to
	s.SetMap(&moved)

	if got := scoredOwner(s, req, eps); got != from {
		t.Fatalf("while warming the bucket went to %q, want its old owner %q", got, from)
	}
	close(srv.release) // the new owner answers the replays
	s.warm.wg.Wait()
	if got := scoredOwner(s, req, eps); got != to {
		t.Fatalf("after warming the bucket went to %q, want its new owner %q", got, to)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.bodies) != 2 {
		t.Fatalf("%d prompts replayed, want the last 2", len(srv.bodies))
	}
	for i, q := range []string{"two", "three"} {
		body := srv.bodies[i]
		msgs := body["messages"].([]any)
		if body["path"] != "/v1/chat/completions" || body["model"] != testModel || body["max_tokens"] != float64(1) ||
			msgs[0].(map[string]any)["content"] != "preamble | document 7 | question "+q {
			t.Fatalf("replay %d: %v", i, body)
		}
	}
	if n := testutil.ToFloat64(s.warm.sent.WithLabelValues(strconv.Itoa(bucket))); n != 2 {
		t.Fatalf("warm requests counted: %v", n)
	}
}

func TestWarmingMovesAtOnceWithoutPrompts(t *testing.T) {
	s := New("", nil)
	s.EnableWarming(0, nil)
	eps := []fwksched.Endpoint{endpointAt("a", "http://127.0.0.1:1"), endpointAt("b", "http://127.0.0.1:2")}
	m, _ := buckets.Assign(nil, []string{"a", "b"}, 16, 2048)
	s.SetMap(m)
	req := chat("never seen before")
	bucket, from, _ := bucketOf(m, req)
	to := map[string]string{"a": "b", "b": "a"}[from]
	s.Score(context.Background(), req, eps)

	moved := *m
	moved.Owners = slices.Clone(m.Owners)
	moved.Owners[bucket] = to
	s.SetMap(&moved)
	if got := scoredOwner(s, req, eps); got != to {
		t.Fatalf("a bucket with nothing to warm went to %q, want %q at once", got, to)
	}
}
