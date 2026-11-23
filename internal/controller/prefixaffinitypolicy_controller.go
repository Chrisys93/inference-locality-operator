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
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1alpha1 "github.com/Chrisys93/inference-locality-operator/api/v1alpha1"
	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
	"github.com/Chrisys93/inference-locality-operator/internal/metrics"
)

const (
	// MapKey is the ConfigMap key holding the encoded bucket map.
	MapKey = "map.json"
	// PolicyLabel marks a bucket-map ConfigMap with the policy that owns it.
	PolicyLabel = "routing.chrisys93.github.io/policy"
	// ConditionReady is true once a map covering every bucket has been published.
	ConditionReady = "Ready"
)

// observation is the controller's memory of one policy between intervals.
type observation struct {
	at      time.Time
	samples map[string]metrics.Sample // pod name -> last reading
}

// PrefixAffinityPolicyReconciler reconciles a PrefixAffinityPolicy object.
type PrefixAffinityPolicyReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Scraper metrics.Scraper
	// Now returns the current time; tests replace it.
	Now func() time.Time

	mu   sync.Mutex
	seen map[types.NamespacedName]*observation
}

// +kubebuilder:rbac:groups=routing.chrisys93.github.io,resources=prefixaffinitypolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.chrisys93.github.io,resources=prefixaffinitypolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=routing.chrisys93.github.io,resources=prefixaffinitypolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch

// Reconcile reads the replicas' metrics once per interval, keeps the bucket map in step with the replicas, moves
// buckets away from replicas the trigger marks as overloaded, publishes the map and reports what it saw.
func (r *PrefixAffinityPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	now := r.now()

	var policy routingv1alpha1.PrefixAffinityPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	spec := withDefaults(policy.Spec)
	interval := spec.Rebalance.Interval.Duration

	replicas, err := r.readyReplicas(ctx, &policy, spec)
	if err != nil {
		return ctrl.Result{}, err
	}
	names := make([]string, len(replicas))
	for i, p := range replicas {
		names[i] = p.Name
	}

	// Metrics: read only when an interval has passed, so reconciles caused by pod or map events do not shorten
	// the measurement window.
	loads, statuses, measured := r.observe(ctx, req.NamespacedName, replicas, spec, now)

	// Map: start from the published one, follow replica changes, then rebalance.
	cm := &corev1.ConfigMap{Name: policy.Name + "-bucket-map", Namespace: policy.Namespace}
	var prev *buckets.Map
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), cm); err == nil {
		if m, derr := buckets.Decode(cm.Data[MapKey]); derr == nil {
			prev = m
		} else {
			log.Info("published bucket map unreadable, rebuilding", "error", derr.Error())
		}
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	m, _ := buckets.Assign(prev, names, int(spec.Buckets), int(spec.Prefix.Bytes))

	status := policy.Status.DeepCopy()
	if measured && len(names) > 1 {
		cfg := triggerConfig(spec)
		cfg.MinBuckets = minBuckets(int(spec.Buckets), len(names), int(*spec.Rebalance.MinSharePercent))
		moves := buckets.Rebalance(m, loads, cfg, now, nil)
		if len(moves) > 0 {
			for _, mv := range moves {
				log.Info("bucket moved", "bucket", mv.Bucket, "from", mv.From, "to", mv.To, "mapVersion", m.Version)
			}
		}
	}

	if err := r.publish(ctx, &policy, cm, m); err != nil {
		return ctrl.Result{}, err
	}

	// Status
	status.ObservedGeneration = policy.Generation
	status.BucketMap = cm.Name
	status.MapVersion = m.Version
	status.TotalMoves = m.Moves
	if m.LastMove > 0 {
		t := metav1.NewTime(time.Unix(m.LastMove, 0))
		status.LastRebalanceTime = &t
	}
	counts := m.Counts()
	if measured {
		for i := range statuses {
			statuses[i].Buckets = int32(counts[statuses[i].Name])
		}
		status.Replicas = statuses
	} else if !sameNames(status.Replicas, names) {
		status.Replicas = nil
		for _, n := range names {
			status.Replicas = append(status.Replicas, routingv1alpha1.ReplicaStatus{Name: n, Buckets: int32(counts[n])})
		}
	}
	cond := metav1.Condition{Type: ConditionReady, ObservedGeneration: policy.Generation}
	if len(names) == 0 {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "NoReplicas", "no ready pod matches the selector"
	} else {
		cond.Status, cond.Reason = metav1.ConditionTrue, "MapPublished"
		cond.Message = fmt.Sprintf("%d buckets over %d replicas", spec.Buckets, len(names))
	}
	meta.SetStatusCondition(&status.Conditions, cond)
	// A patch, not an update: every field is recomputed from the published map and fresh readings, so a
	// cached copy that is one write behind must not make the write fail.
	if !equality.Semantic.DeepEqual(status, &policy.Status) {
		base := policy.DeepCopy()
		policy.Status = *status
		if err := r.Status().Patch(ctx, &policy, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: r.untilDue(req.NamespacedName, interval, now)}, nil
}

func (r *PrefixAffinityPolicyReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *PrefixAffinityPolicyReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.seen, key)
}

// untilDue is the time left before the next metrics reading is due.
func (r *PrefixAffinityPolicyReconciler) untilDue(key types.NamespacedName, interval time.Duration, now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if o := r.seen[key]; o != nil {
		if d := o.at.Add(interval).Sub(now); d > 0 {
			return d
		}
	}
	return interval
}

// readyReplicas lists the ready pods matching the selector, sorted by name.
func (r *PrefixAffinityPolicyReconciler) readyReplicas(ctx context.Context, p *routingv1alpha1.PrefixAffinityPolicy,
	spec routingv1alpha1.PrefixAffinityPolicySpec) ([]corev1.Pod, error) {
	sel, err := metav1.LabelSelectorAsSelector(&spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}
	if sel.Empty() {
		return nil, fmt.Errorf("selector must not be empty")
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(p.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	var ready []corev1.Pod
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil && pod.Status.PodIP != "" && podReady(&pod) {
			ready = append(ready, pod)
		}
	}
	slices.SortFunc(ready, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return ready, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// observe reads every replica's metrics if an interval has passed since the last reading. measured is true when
// it did; loads then carry figures for the replicas that had a previous reading.
func (r *PrefixAffinityPolicyReconciler) observe(ctx context.Context, key types.NamespacedName, pods []corev1.Pod,
	spec routingv1alpha1.PrefixAffinityPolicySpec, now time.Time) ([]buckets.Load, []routingv1alpha1.ReplicaStatus, bool) {
	r.mu.Lock()
	if r.seen == nil {
		r.seen = map[types.NamespacedName]*observation{}
	}
	prev := r.seen[key]
	r.mu.Unlock()
	// a small tolerance, so a requeue that fires a little early still counts as due
	if prev != nil && now.Sub(prev.at) < spec.Rebalance.Interval.Duration*9/10 {
		return nil, nil, false
	}

	cfg := triggerConfig(spec)
	cur := &observation{at: now, samples: map[string]metrics.Sample{}}
	loads := make([]buckets.Load, 0, len(pods))
	statuses := make([]routingv1alpha1.ReplicaStatus, 0, len(pods))
	for _, p := range pods {
		url := fmt.Sprintf("http://%s:%d%s", p.Status.PodIP, spec.MetricsPort, spec.MetricsPath)
		st := routingv1alpha1.ReplicaStatus{Name: p.Name}
		l := buckets.Load{Name: p.Name}
		sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		s, err := r.Scraper.Scrape(sctx, url)
		cancel()
		if err != nil {
			st.ScrapeError = err.Error()
		} else {
			cur.samples[p.Name] = s
			st.KVCacheUsagePercent = int32(s.Values[metrics.KVCacheUsage]*100 + 0.5)
			l.KVCacheUsage = s.Values[metrics.KVCacheUsage]
			if prev != nil {
				if ps, ok := prev.samples[p.Name]; ok {
					iv := metrics.Diff(ps, s)
					l.OK = true
					l.Requests = iv.Requests
					l.MeanQueueSec = iv.MeanQueueSec
					l.MeanServiceSec = iv.MeanServiceSec
					st.Requests = iv.Requests
					st.MeanQueueMillis = int64(iv.MeanQueueSec*1000 + 0.5)
					st.MeanServiceMillis = int64(iv.MeanServiceSec*1000 + 0.5)
				}
			}
			if cfg.Trigger == buckets.KVCacheUsage {
				l.OK = true // the KV trigger needs only the current reading
			}
		}
		st.Overloaded = cfg.Overloaded(l)
		loads = append(loads, l)
		statuses = append(statuses, st)
	}
	r.mu.Lock()
	r.seen[key] = cur
	r.mu.Unlock()
	return loads, statuses, true
}

// publish writes the map to the ConfigMap, owned by the policy, if it differs from what is there.
func (r *PrefixAffinityPolicyReconciler) publish(ctx context.Context, p *routingv1alpha1.PrefixAffinityPolicy,
	cm *corev1.ConfigMap, m *buckets.Map) error {
	data, err := m.Encode()
	if err != nil {
		return err
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels[PolicyLabel] = p.Name
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[MapKey] = data
		return controllerutil.SetControllerReference(p, cm, r.Scheme)
	})
	return err
}

// minBuckets is minSharePercent of the fair share, rounded up.
func minBuckets(total, replicas, pct int) int {
	if replicas == 0 {
		return 1
	}
	return (total*pct + replicas*100 - 1) / (replicas * 100)
}

func sameNames(st []routingv1alpha1.ReplicaStatus, names []string) bool {
	if len(st) != len(names) {
		return false
	}
	for i := range st {
		if st[i].Name != names[i] {
			return false
		}
	}
	return true
}

func triggerConfig(spec routingv1alpha1.PrefixAffinityPolicySpec) buckets.Config {
	return buckets.Config{
		Trigger:             buckets.Trigger(spec.Trigger.Type),
		QueueDelayPercent:   int(spec.Trigger.QueueDelayPercent),
		KVCacheUsagePercent: int(spec.Trigger.KVCacheUsagePercent),
		MinRequests:         int64(spec.Trigger.MinRequests),
		MaxMoves:            int(spec.Rebalance.MaxMovesPerInterval),
		Cooldown:            spec.Rebalance.Cooldown.Duration,
	}
}

// withDefaults fills fields left empty. The API server applies the same defaults; this covers objects built in
// code. maxMovesPerInterval is left alone, since 0 (never move) is a valid choice.
func withDefaults(s routingv1alpha1.PrefixAffinityPolicySpec) routingv1alpha1.PrefixAffinityPolicySpec {
	if s.MetricsPort == 0 {
		s.MetricsPort = 8000
	}
	if s.MetricsPath == "" {
		s.MetricsPath = "/metrics"
	}
	if s.Buckets == 0 {
		s.Buckets = 64
	}
	if s.Prefix.Bytes == 0 {
		s.Prefix.Bytes = 2048
	}
	if s.Trigger.Type == "" {
		s.Trigger.Type = routingv1alpha1.TriggerQueueDelay
	}
	if s.Trigger.QueueDelayPercent == 0 {
		s.Trigger.QueueDelayPercent = 200
	}
	if s.Trigger.KVCacheUsagePercent == 0 {
		s.Trigger.KVCacheUsagePercent = 90
	}
	if s.Trigger.MinRequests == 0 {
		s.Trigger.MinRequests = 20
	}
	if s.Rebalance.Interval.Duration == 0 {
		s.Rebalance.Interval.Duration = 30 * time.Second
	}
	if s.Rebalance.MinSharePercent == nil {
		pct := int32(25)
		s.Rebalance.MinSharePercent = &pct
	}
	if s.Rebalance.Cooldown.Duration == 0 {
		s.Rebalance.Cooldown.Duration = 2 * time.Minute
	}
	return s
}

// policiesForPod maps a pod event to the policies in its namespace whose selector matches it.
func (r *PrefixAffinityPolicyReconciler) policiesForPod(ctx context.Context, o client.Object) []reconcile.Request {
	var list routingv1alpha1.PrefixAffinityPolicyList
	if err := r.List(ctx, &list, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, p := range list.Items {
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.Selector)
		if err != nil || sel.Empty() || !sel.Matches(labels.Set(o.GetLabels())) {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&p)})
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *PrefixAffinityPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Scraper == nil {
		r.Scraper = metrics.HTTPScraper{}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1alpha1.PrefixAffinityPolicy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.ConfigMap{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.policiesForPod)).
		Named("prefixaffinitypolicy").
		Complete(r)
}
