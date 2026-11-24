package bucketaffinity

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func endpoint(name string) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{Name: name}, nil, nil)
}

func chat(text string) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{Body: &fwkrh.InferenceRequestBody{ChatCompletions: &fwkrh.ChatCompletionsRequest{
		Messages: []fwkrh.Message{{Role: "user", Content: fwkrh.Content{Raw: text}}},
	}}}
}

func TestScoreFollowsTheMap(t *testing.T) {
	s := New("", nil)
	a, b := endpoint("a"), endpoint("b")
	req := chat("preamble | document 7 | question")

	for e, v := range s.Score(context.Background(), req, []fwksched.Endpoint{a, b}) {
		if v != 0 {
			t.Fatalf("without a map %s scored %v", e.GetMetadata().Name, v)
		}
	}

	m, _ := buckets.Assign(nil, []string{"a", "b"}, 16, 2048)
	s.SetMap(m)
	text, _ := PromptText(req.Body)
	owner := m.Owners[buckets.Key(text, m.PrefixBytes, m.Buckets)]
	got := s.Score(context.Background(), req, []fwksched.Endpoint{a, b})
	for e, v := range got {
		want := 0.0
		if e.GetMetadata().Name == owner {
			want = 1
		}
		if v != want {
			t.Fatalf("%s scored %v, owner is %s", e.GetMetadata().Name, v, owner)
		}
	}

	// the owner left the candidate list: nobody is preferred
	other := map[string]fwksched.Endpoint{"a": b, "b": a}[owner]
	if v := s.Score(context.Background(), req, []fwksched.Endpoint{other})[other]; v != 0 {
		t.Fatalf("non-owner scored %v", v)
	}
}

func TestSamePrefixSameBucket(t *testing.T) {
	m, _ := buckets.Assign(nil, []string{"a", "b", "c"}, 64, 40)
	_, o1, _ := bucketOf(m, chat("a shared preamble of forty bytes or more, then question one"))
	_, o2, _ := bucketOf(m, chat("a shared preamble of forty bytes or more, then question two"))
	if o1 != o2 {
		t.Fatalf("owners %s and %s for the same prefix", o1, o2)
	}
}

func TestPromptText(t *testing.T) {
	completions := &fwkrh.CompletionsRequest{Prompt: fwkrh.Prompt{Raw: "hi"}}
	if s, ok := PromptText(&fwkrh.InferenceRequestBody{Completions: completions}); !ok || s != "hi" {
		t.Fatalf("completions: %q %v", s, ok)
	}
	body := &fwkrh.InferenceRequestBody{ChatCompletions: &fwkrh.ChatCompletionsRequest{Messages: []fwkrh.Message{
		{Role: "system", Content: fwkrh.Content{Raw: "be brief"}},
		{Role: "user", Content: fwkrh.Content{Structured: []fwkrh.ContentBlock{{Type: "text", Text: "why"}}}},
	}}}
	if s, ok := PromptText(body); !ok || s != "system\nbe brief\nuser\nwhy \n" {
		t.Fatalf("chat: %q %v", s, ok)
	}
	if _, ok := PromptText(&fwkrh.InferenceRequestBody{}); ok {
		t.Fatal("empty body gave a prompt")
	}
}

func TestResponseBodyCountsPerBucket(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New("", reg)
	m, _ := buckets.Assign(nil, []string{"a", "b"}, 8, 2048)
	s.SetMap(m)
	req := chat("doc 3 | question")
	b, owner, _ := bucketOf(m, req)
	notOwner := map[string]string{"a": "b", "b": "a"}[owner]
	usage := fwkrh.Usage{PromptTokens: 100, PromptTokenDetails: &fwkrh.PromptTokenDetails{CachedTokens: 80}}

	byOwner := &fwkdl.EndpointMetadata{Name: owner}
	s.ResponseBody(context.Background(), req, &fwkrc.Response{Usage: usage}, byOwner) // mid-stream: not counted
	s.ResponseBody(context.Background(), req, &fwkrc.Response{EndOfStream: true, Usage: usage}, byOwner)
	s.ResponseBody(context.Background(), req, &fwkrc.Response{EndOfStream: true, Usage: fwkrh.Usage{PromptTokens: 100}},
		&fwkdl.EndpointMetadata{Name: notOwner})

	l := []string{strconv.Itoa(b)}
	for name, c := range map[string]struct {
		v    float64
		want float64
	}{
		"requests":      {testutil.ToFloat64(s.stats.requests.WithLabelValues(l...)), 2},
		"owner served":  {testutil.ToFloat64(s.stats.ownerServed.WithLabelValues(l...)), 1},
		"prompt tokens": {testutil.ToFloat64(s.stats.promptTokens.WithLabelValues(l...)), 200},
		"cached tokens": {testutil.ToFloat64(s.stats.cachedTokens.WithLabelValues(l...)), 80},
	} {
		if c.v != c.want {
			t.Errorf("%s: %v, want %v", name, c.v, c.want)
		}
	}
	if n, err := testutil.GatherAndCount(reg, "ilo_bucket_requests_total"); err != nil || n != 1 {
		t.Fatalf("registered series: %d %v", n, err)
	}
}
