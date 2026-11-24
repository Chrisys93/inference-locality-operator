/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/Chrisys93/inference-locality-operator/api/v1alpha1"
	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	"github.com/Chrisys93/inference-locality-operator/internal/metrics"
)

// fakeScraper returns cumulative counters per pod IP; tests advance them between intervals.
type fakeScraper struct {
	mu   sync.Mutex
	vals map[string]map[string]float64 // ip -> series -> value
	fail map[string]bool
}

func (f *fakeScraper) Scrape(_ context.Context, url string) (metrics.Sample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ip, _, _ := strings.Cut(strings.TrimPrefix(url, "http://"), ":")
	if f.fail[ip] {
		return metrics.Sample{}, fmt.Errorf("connection refused")
	}
	v := map[string]float64{}
	maps.Copy(v, f.vals[ip])
	return metrics.Sample{Time: time.Now(), Values: v}, nil
}

// serve adds n completed requests to a pod with the given mean queue and service times.
func (f *fakeScraper) serve(ip string, n int, queueSec, serviceSec float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.vals == nil {
		f.vals = map[string]map[string]float64{}
	}
	if f.vals[ip] == nil {
		f.vals[ip] = map[string]float64{}
	}
	f.vals[ip][metrics.InferenceCount] += float64(n)
	f.vals[ip][metrics.InferenceTimeSum] += float64(n) * serviceSec
	f.vals[ip][metrics.QueueTimeSum] += float64(n) * queueSec
}

// fakeBuckets returns cumulative per-bucket counters and records the token it was sent.
type fakeBuckets struct {
	mu     sync.Mutex
	counts map[int]metrics.BucketCounts
	url    string
	bearer string
}

func (f *fakeBuckets) ScrapeBuckets(_ context.Context, url, bearer string) (map[int]metrics.BucketCounts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.url, f.bearer = url, bearer
	out := map[int]metrics.BucketCounts{}
	maps.Copy(out, f.counts)
	return out, nil
}

func (f *fakeBuckets) add(bucket int, requests float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts == nil {
		f.counts = map[int]metrics.BucketCounts{}
	}
	c := f.counts[bucket]
	c.Requests += requests
	c.OwnerServed += requests
	c.PromptTokens += 100 * requests
	c.CachedTokens += 90 * requests
	f.counts[bucket] = c
}

var _ = Describe("PrefixAffinityPolicy Controller", func() {
	const ns = "default"
	ctx := context.Background()

	var (
		scraper       *fakeScraper
		bucketScraper *fakeBuckets
		clock         time.Time
		rec           *PrefixAffinityPolicyReconciler
	)

	makePod := func(name, app, ip string) {
		pod := &corev1.Pod{
			Name: name, Namespace: ns, Labels: map[string]string{"app": app},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sim", Image: "sim"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status = corev1.PodStatus{
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	deletePod := func(name string) {
		pod := &corev1.Pod{Name: name, Namespace: ns}
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		Eventually(func() bool {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod) != nil
		}).Should(BeTrue())
	}

	makePolicy := func(name, app string) types.NamespacedName {
		p := &routingv1alpha1.PrefixAffinityPolicy{
			Name: name, Namespace: ns,
			Spec: routingv1alpha1.PrefixAffinityPolicySpec{
				Selector:  metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
				Buckets:   32,
				Rebalance: routingv1alpha1.RebalanceSpec{MaxMovesPerInterval: 4},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		return client.ObjectKeyFromObject(p)
	}

	reconcileAt := func(key types.NamespacedName, t time.Time) reconcile.Result {
		clock = t
		res, err := rec.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		return res
	}

	getMap := func(key types.NamespacedName) *buckets.Map {
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: key.Name + "-bucket-map"}, cm)).To(Succeed())
		m, err := buckets.Decode(cm.Data[MapKey])
		Expect(err).NotTo(HaveOccurred())
		return m
	}

	getPolicy := func(key types.NamespacedName) *routingv1alpha1.PrefixAffinityPolicy {
		p := &routingv1alpha1.PrefixAffinityPolicy{}
		Expect(k8sClient.Get(ctx, key, p)).To(Succeed())
		return p
	}

	BeforeEach(func() {
		scraper = &fakeScraper{fail: map[string]bool{}}
		clock = time.Unix(1_000_000, 0)
		bucketScraper = &fakeBuckets{}
		tokenFile := filepath.Join(GinkgoT().TempDir(), "token")
		Expect(os.WriteFile(tokenFile, []byte("test-token\n"), 0o600)).To(Succeed())
		rec = &PrefixAffinityPolicyReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			Scraper:       scraper,
			BucketScraper: bucketScraper,
			TokenFile:     tokenFile,
			Now:           func() time.Time { return clock },
		}
	})

	It("publishes a map over the ready replicas and reports Ready", func() {
		makePod("a1", "pool-a", "10.0.0.1")
		makePod("a2", "pool-a", "10.0.0.2")
		makePod("other", "elsewhere", "10.0.0.9")
		key := makePolicy("publish", "pool-a")

		res := reconcileAt(key, clock)
		Expect(res.RequeueAfter).To(Equal(30 * time.Second))

		m := getMap(key)
		Expect(m.Owners).To(HaveLen(32))
		Expect(m.PrefixBytes).To(Equal(2048))
		counts := m.Counts()
		Expect(counts).To(HaveLen(2))
		Expect(counts["a1"] + counts["a2"]).To(Equal(32))

		p := getPolicy(key)
		Expect(p.Status.MapVersion).To(Equal(int64(1)))
		Expect(p.Status.Replicas).To(HaveLen(2))
		Expect(meta.IsStatusConditionTrue(p.Status.Conditions, ConditionReady)).To(BeTrue())

		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "publish-bucket-map"}, cm)).To(Succeed())
		Expect(metav1.IsControlledBy(cm, p)).To(BeTrue())
	})

	It("moves buckets away from a replica whose queueing delay passes the trigger", func() {
		makePod("b1", "pool-b", "10.0.1.1")
		makePod("b2", "pool-b", "10.0.1.2")
		key := makePolicy("rebalance", "pool-b")
		t0 := clock

		reconcileAt(key, t0) // first reading, no interval yet
		before := getMap(key).Counts()

		// a reconcile before the interval is over neither reads nor moves
		scraper.serve("10.0.1.1", 100, 1.0, 0.2)
		scraper.serve("10.0.1.2", 100, 0.0, 0.2)
		reconcileAt(key, t0.Add(5*time.Second))
		Expect(getPolicy(key).Status.TotalMoves).To(BeZero())

		res := reconcileAt(key, t0.Add(30*time.Second))
		Expect(res.RequeueAfter).To(Equal(30 * time.Second))
		after := getMap(key)
		Expect(after.Counts()["b1"]).To(Equal(before["b1"] - 4))
		Expect(after.Counts()["b2"]).To(Equal(before["b2"] + 4))
		Expect(after.Version).To(Equal(int64(2)))

		p := getPolicy(key)
		Expect(p.Status.TotalMoves).To(Equal(int64(4)))
		Expect(p.Status.LastRebalanceTime).NotTo(BeNil())
		var b1 routingv1alpha1.ReplicaStatus
		for _, r := range p.Status.Replicas {
			if r.Name == "b1" {
				b1 = r
			}
		}
		Expect(b1.Overloaded).To(BeTrue())
		Expect(b1.Requests).To(Equal(int64(100)))
		Expect(b1.MeanQueueMillis).To(Equal(int64(1000)))
		Expect(b1.MeanServiceMillis).To(Equal(int64(200)))

		// still overloaded, with traffic that does not follow the buckets: moves stop at the floor
		// (25% of a fair share of 16 = 4 buckets)
		for i := 2; i <= 12; i++ {
			scraper.serve("10.0.1.1", 100, 1.0, 0.2)
			scraper.serve("10.0.1.2", 100, 0.0, 0.2)
			reconcileAt(key, t0.Add(time.Duration(i)*30*time.Second))
		}
		Expect(getMap(key).Counts()["b1"]).To(Equal(4))
		t0 = t0.Add(11 * 30 * time.Second)

		// calm interval: nothing moves
		scraper.serve("10.0.1.1", 100, 0.1, 0.3) // service times differ between pods and intervals
		scraper.serve("10.0.1.2", 100, 0.1, 0.25)
		moves, version := getPolicy(key).Status.TotalMoves, getMap(key).Version
		reconcileAt(key, t0.Add(60*time.Second))
		Expect(getPolicy(key).Status.TotalMoves).To(Equal(moves))
		Expect(getMap(key).Version).To(Equal(version))

		// the map deleted by hand: rebuilt from scratch, and the status forgets the earlier moves
		Expect(k8sClient.Delete(ctx, &corev1.ConfigMap{Name: "rebalance-bucket-map", Namespace: ns})).To(Succeed())
		reconcileAt(key, t0.Add(61*time.Second))
		p = getPolicy(key)
		Expect(p.Status.TotalMoves).To(BeZero())
		Expect(p.Status.LastRebalanceTime).To(BeNil())
		Expect(getMap(key).Counts()["b1"]).To(Equal(before["b1"]))
	})

	It("moves the busiest bucket that fits, using the scorer's counters", func() {
		makePod("s1", "pool-s", "10.0.5.1")
		makePod("s2", "pool-s", "10.0.5.2")
		svc := &corev1.Service{Name: "epp", Namespace: ns,
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "metrics", Port: 9090}}}}
		Expect(k8sClient.Create(ctx, svc)).To(Succeed())
		key := makePolicy("by-load", "pool-s")
		p := getPolicy(key)
		p.Spec.ScorerMetrics = &routingv1alpha1.ScorerMetricsSpec{Service: "epp",
			Auth: routingv1alpha1.ScorerAuthServiceAccountToken}
		Expect(k8sClient.Update(ctx, p)).To(Succeed())
		t0 := clock
		reconcileAt(key, t0)
		m := getMap(key)
		var hot []int
		for b, o := range m.Owners {
			if o == "s1" {
				hot = append(hot, b)
			}
		}
		// s1: one dominant bucket (300), one of 60, one of 20; s2 idle
		bucketScraper.add(hot[0], 300)
		bucketScraper.add(hot[1], 60)
		bucketScraper.add(hot[2], 20)
		scraper.serve("10.0.5.1", 380, 1.0, 0.2)
		scraper.serve("10.0.5.2", 20, 0.0, 0.2)
		reconcileAt(key, t0.Add(30*time.Second))

		// 300 would leave s2 busier than s1 (300 vs 80): it stays; 60 and 20 move
		after := getMap(key)
		Expect(after.Owners[hot[0]]).To(Equal("s1"))
		Expect(after.Owners[hot[1]]).To(Equal("s2"))
		Expect(after.Owners[hot[2]]).To(Equal("s2"))
		Expect(after.Moves).To(Equal(int64(2)))
		Expect(bucketScraper.url).To(Equal("http://epp.default.svc:9090/metrics"))
		Expect(bucketScraper.bearer).To(Equal("test-token"))

		st := getPolicy(key).Status
		Expect(st.ScorerMetricsError).To(BeEmpty())
		Expect(st.BusiestBuckets).To(HaveLen(3))
		Expect(st.BusiestBuckets[0]).To(Equal(routingv1alpha1.BucketStatus{
			Bucket: int32(hot[0]), Owner: "s1", Requests: 300, CachedPercent: 90, OwnerServedPercent: 100}))
		Expect(st.BusiestBuckets[1].Owner).To(Equal("s2"))
	})

	It("waits for per-bucket counts rather than move blind", func() {
		makePod("w1", "pool-w", "10.0.7.1")
		makePod("w2", "pool-w", "10.0.7.2")
		key := makePolicy("wait-counts", "pool-w")
		t0 := clock
		reconcileAt(key, t0) // first reading, without scorerMetrics

		svc := &corev1.Service{Name: "epp-w", Namespace: ns,
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "metrics", Port: 9090}}}}
		Expect(k8sClient.Create(ctx, svc)).To(Succeed())
		p := getPolicy(key)
		p.Spec.ScorerMetrics = &routingv1alpha1.ScorerMetricsSpec{Service: "epp-w"}
		Expect(k8sClient.Update(ctx, p)).To(Succeed())

		// overloaded, but the scorer has been read only once: no difference to take yet, so nothing moves
		scraper.serve("10.0.7.1", 100, 1.0, 0.2)
		scraper.serve("10.0.7.2", 100, 0.0, 0.2)
		reconcileAt(key, t0.Add(30*time.Second))
		Expect(getMap(key).Moves).To(BeZero())
		Expect(bucketScraper.bearer).To(BeEmpty()) // auth None

		// next interval: counts are there, the busy bucket moves
		var hot int
		for b, o := range getMap(key).Owners {
			if o == "w1" {
				hot = b
				break
			}
		}
		bucketScraper.add(hot, 30)
		for b, o := range getMap(key).Owners {
			if o == "w1" && b != hot {
				bucketScraper.add(b, 60)
				break
			}
		}
		scraper.serve("10.0.7.1", 100, 1.0, 0.2)
		scraper.serve("10.0.7.2", 100, 0.0, 0.2)
		reconcileAt(key, t0.Add(60*time.Second))
		Expect(getMap(key).Moves).To(Equal(int64(1)))
		Expect(getMap(key).Owners[hot]).To(Equal("w2"))
	})

	It("does not send its token to an ExternalName service", func() {
		makePod("x1", "pool-x", "10.0.6.1")
		svc := &corev1.Service{Name: "outside", Namespace: ns,
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: "example.org"}}
		Expect(k8sClient.Create(ctx, svc)).To(Succeed())
		key := makePolicy("external", "pool-x")
		p := getPolicy(key)
		p.Spec.ScorerMetrics = &routingv1alpha1.ScorerMetricsSpec{Service: "outside",
			Auth: routingv1alpha1.ScorerAuthServiceAccountToken}
		Expect(k8sClient.Update(ctx, p)).To(Succeed())
		reconcileAt(key, clock)
		Expect(getPolicy(key).Status.ScorerMetricsError).To(ContainSubstring("ExternalName"))
		Expect(bucketScraper.bearer).To(BeEmpty())
	})

	It("does not judge a replica whose metrics cannot be read", func() {
		makePod("c1", "pool-c", "10.0.2.1")
		makePod("c2", "pool-c", "10.0.2.2")
		key := makePolicy("scrape-error", "pool-c")
		reconcileAt(key, clock)
		scraper.serve("10.0.2.1", 40, 1.0, 0.2)
		scraper.fail["10.0.2.2"] = true // the only possible target is unreadable
		reconcileAt(key, clock.Add(30*time.Second))
		p := getPolicy(key)
		Expect(p.Status.TotalMoves).To(BeZero())
		var c2 routingv1alpha1.ReplicaStatus
		for _, r := range p.Status.Replicas {
			if r.Name == "c2" {
				c2 = r
			}
		}
		Expect(c2.ScrapeError).NotTo(BeEmpty())
	})

	It("follows replicas leaving and joining", func() {
		makePod("d1", "pool-d", "10.0.3.1")
		makePod("d2", "pool-d", "10.0.3.2")
		key := makePolicy("membership", "pool-d")
		reconcileAt(key, clock)

		deletePod("d2")
		reconcileAt(key, clock.Add(time.Second))
		m := getMap(key)
		Expect(m.Counts()).To(Equal(map[string]int{"d1": 32}))

		makePod("d3", "pool-d", "10.0.3.3")
		reconcileAt(key, clock.Add(time.Second))
		m2 := getMap(key)
		Expect(m2.Counts()["d3"]).To(BeNumerically(">", 0))
		Expect(m2.Version).To(BeNumerically(">", m.Version))
	})

	It("reports no replicas when none is ready", func() {
		key := makePolicy("empty", "pool-none")
		reconcileAt(key, clock)
		p := getPolicy(key)
		Expect(meta.IsStatusConditionFalse(p.Status.Conditions, ConditionReady)).To(BeTrue())
	})

	It("forgets a deleted policy", func() {
		key := makePolicy("gone", "pool-none")
		reconcileAt(key, clock)
		Expect(k8sClient.Delete(ctx, getPolicy(key))).To(Succeed())
		reconcileAt(key, clock.Add(time.Second))
		rec.mu.Lock()
		_, seen := rec.seen[key]
		rec.mu.Unlock()
		Expect(seen).To(BeFalse())
	})
})
