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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TriggerType selects the signal that marks a replica as overloaded.
// +kubebuilder:validation:Enum=QueueDelay;KVCacheUsage
type TriggerType string

const (
	// TriggerQueueDelay fires when a replica's mean queueing delay exceeds a multiple of its mean service time
	// (the ReStorEdge queueing trigger).
	TriggerQueueDelay TriggerType = "QueueDelay"
	// TriggerKVCacheUsage fires when a replica's KV-cache use exceeds a threshold.
	TriggerKVCacheUsage TriggerType = "KVCacheUsage"
)

// PrefixSpec defines the part of the prompt that names a bucket.
type PrefixSpec struct {
	// bytes is the number of leading bytes of the prompt (UTF-8) hashed into the bucket key. The scorer reads the
	// value from the published bucket map, so both use the same.
	// +kubebuilder:default=2048
	// +kubebuilder:validation:Minimum=16
	// +optional
	Bytes int32 `json:"bytes,omitempty"`
}

// TriggerSpec defines when a replica counts as overloaded.
type TriggerSpec struct {
	// type of the trigger.
	// +kubebuilder:default=QueueDelay
	// +optional
	Type TriggerType `json:"type,omitempty"`

	// queueDelayPercent: QueueDelay fires when mean queueing delay > queueDelayPercent/100 x mean service time
	// over the last interval. 200 is the "twice the service time" trigger.
	// +kubebuilder:default=200
	// +kubebuilder:validation:Minimum=1
	// +optional
	QueueDelayPercent int32 `json:"queueDelayPercent,omitempty"`

	// kvCacheUsagePercent: KVCacheUsage fires when the replica reports a KV-cache use above this percentage.
	// +kubebuilder:default=90
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	KVCacheUsagePercent int32 `json:"kvCacheUsagePercent,omitempty"`

	// minRequests is the number of requests a replica must have completed in the interval for its QueueDelay
	// figures to count; below it the replica is not judged.
	// +kubebuilder:default=20
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinRequests int32 `json:"minRequests,omitempty"`
}

// RebalanceSpec bounds how buckets move.
type RebalanceSpec struct {
	// interval between metric reads and rebalancing decisions.
	// +kubebuilder:default="30s"
	// +optional
	Interval metav1.Duration `json:"interval,omitempty"`

	// maxMovesPerInterval caps the buckets moved in one interval.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxMovesPerInterval int32 `json:"maxMovesPerInterval,omitempty"`

	// minSharePercent is the floor on how far rebalancing can drain a replica: it keeps at least this percentage
	// of its fair share (buckets / replicas), and never less than one bucket. Without a floor, a replica whose
	// load does not follow its buckets (traffic that bypasses the router) would be stripped bare.
	// +kubebuilder:default=25
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	MinSharePercent *int32 `json:"minSharePercent,omitempty"`

	// cooldown is the time a moved bucket stays with its new owner before it can move again.
	// +kubebuilder:default="2m"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitempty"`
}

// PrefixAffinityPolicySpec defines the desired state of PrefixAffinityPolicy.
type PrefixAffinityPolicySpec struct {
	// selector picks the model-server pods (in the policy's namespace) whose prefix space is shared out; normally
	// the same labels as the InferencePool's selector.
	// +required
	Selector metav1.LabelSelector `json:"selector"`

	// metricsPort is the port serving Prometheus metrics on each model-server pod.
	// +kubebuilder:default=8000
	// +optional
	MetricsPort int32 `json:"metricsPort,omitempty"`

	// metricsPath is the path of the metrics endpoint.
	// +kubebuilder:default="/metrics"
	// +optional
	MetricsPath string `json:"metricsPath,omitempty"`

	// buckets is the number of prefix-hash buckets shared out among the replicas.
	// +kubebuilder:default=64
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	Buckets int32 `json:"buckets,omitempty"`

	// +optional
	Prefix PrefixSpec `json:"prefix,omitempty"`

	// +optional
	Trigger TriggerSpec `json:"trigger,omitempty"`

	// +optional
	Rebalance RebalanceSpec `json:"rebalance,omitempty"`
}

// ReplicaStatus is what the controller saw of one replica in the last interval.
type ReplicaStatus struct {
	// name of the pod.
	Name string `json:"name"`
	// buckets owned.
	Buckets int32 `json:"buckets"`
	// requests completed in the last interval.
	Requests int64 `json:"requests"`
	// meanQueueMillis is the mean queueing delay per completed request in the last interval.
	MeanQueueMillis int64 `json:"meanQueueMillis"`
	// meanServiceMillis is the mean inference (service) time per completed request in the last interval.
	MeanServiceMillis int64 `json:"meanServiceMillis"`
	// kvCacheUsagePercent at the last read.
	KVCacheUsagePercent int32 `json:"kvCacheUsagePercent"`
	// overloaded is true when the trigger fired for this replica in the last interval.
	Overloaded bool `json:"overloaded"`
	// scrapeError is set when the metrics could not be read.
	// +optional
	ScrapeError string `json:"scrapeError,omitempty"`
}

// PrefixAffinityPolicyStatus defines the observed state of PrefixAffinityPolicy.
type PrefixAffinityPolicyStatus struct {
	// observedGeneration is the generation of the spec last acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// bucketMap is the name of the ConfigMap holding the bucket-to-replica map.
	// +optional
	BucketMap string `json:"bucketMap,omitempty"`

	// mapVersion increases every time the map changes.
	// +optional
	MapVersion int64 `json:"mapVersion,omitempty"`

	// replicas as seen in the last interval.
	// +optional
	Replicas []ReplicaStatus `json:"replicas,omitempty"`

	// lastRebalanceTime is when buckets last moved because of the trigger.
	// +optional
	LastRebalanceTime *metav1.Time `json:"lastRebalanceTime,omitempty"`

	// totalMoves counts the buckets moved by the trigger since the policy was created.
	// +optional
	TotalMoves int64 `json:"totalMoves,omitempty"`

	// conditions: "Ready" is true when a map covering every bucket has been published.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Buckets",type=integer,JSONPath=`.spec.buckets`
// +kubebuilder:printcolumn:name="Map version",type=integer,JSONPath=`.status.mapVersion`
// +kubebuilder:printcolumn:name="Moves",type=integer,JSONPath=`.status.totalMoves`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PrefixAffinityPolicy is the Schema for the prefixaffinitypolicies API
type PrefixAffinityPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PrefixAffinityPolicy
	// +required
	Spec PrefixAffinityPolicySpec `json:"spec"`

	// status defines the observed state of PrefixAffinityPolicy
	// +optional
	Status PrefixAffinityPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PrefixAffinityPolicyList contains a list of PrefixAffinityPolicy
type PrefixAffinityPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PrefixAffinityPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PrefixAffinityPolicy{}, &PrefixAffinityPolicyList{})
		return nil
	})
}
