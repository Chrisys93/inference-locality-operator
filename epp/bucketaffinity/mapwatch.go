package bucketaffinity

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/Chrisys93/inference-locality-operator/internal/buckets"
)

// watchMap watches the one ConfigMap holding the bucket map and calls set with each new map (nil when the
// ConfigMap is deleted). A map that does not decode, or uses another key function, is logged and ignored, so the
// last good map stays in use.
func watchMap(ctx context.Context, namespace, name string, set func(*buckets.Map)) error {
	cfg, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	logger := log.FromContext(ctx).WithValues("plugin", Type, "configMap", namespace+"/"+name)
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 0, informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
		}))
	inf := factory.Core().V1().ConfigMaps().Informer()

	var version int64 = -1
	apply := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok {
			return
		}
		m, err := buckets.Decode(cm.Data[MapKey])
		if err == nil && m.Hash != buckets.HashName {
			err = fmt.Errorf("key function %q, this scorer uses %q", m.Hash, buckets.HashName)
		}
		if err != nil {
			logger.Error(err, "ignoring bucket map")
			return
		}
		set(m)
		if m.Version != version {
			version = m.Version
			logger.Info("bucket map", "version", m.Version, "buckets", m.Buckets, "owners", m.Counts())
		}
	}
	_, err = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    apply,
		UpdateFunc: func(_, obj any) { apply(obj) },
		DeleteFunc: func(any) {
			set(nil)
			version = -1
			logger.Info("bucket map deleted; scoring stops until it is published again")
		},
	})
	if err != nil {
		return err
	}
	factory.Start(ctx.Done())
	return nil
}
