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

var _ = Describe("PrefixAffinityPolicy Controller", func() {
	const ns = "default"
	ctx := context.Background()

	var (
		scraper *fakeScraper
		clock   time.Time
		rec     *PrefixAffinityPolicyReconciler
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
		rec = &PrefixAffinityPolicyReconciler{
			Client:  k8sClient,
			Scheme:  k8sClient.Scheme(),
			Scraper: scraper,
			Now:     func() time.Time { return clock },
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
