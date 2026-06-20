// Package static implements a provider.Source that engages a fixed set of
// clusters from local kubeconfig files (the --kubeconfigs flag). It is the
// default, kcp-free engagement path.
package static

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// DefaultQPS and DefaultBurst raise client-side rate limits for bulk syncing.
const (
	DefaultQPS   = 100
	DefaultBurst = 200
)

// Source engages each named kubeconfig as a cluster and holds them until the
// context is cancelled.
type Source struct {
	// Kubeconfigs maps cluster name -> kubeconfig file path.
	Kubeconfigs map[string]string
	// QPS and Burst override the client rate limits (0 uses the defaults).
	QPS   float32
	Burst int
}

// Run implements provider.Source. It engages every kubeconfig once, then blocks
// until ctx is cancelled, at which point it disengages all clusters.
func (s *Source) Run(ctx context.Context, aware multicluster.Aware) error {
	logger := klog.FromContext(ctx)

	qps := s.QPS
	if qps == 0 {
		qps = DefaultQPS
	}
	burst := s.Burst
	if burst == 0 {
		burst = DefaultBurst
	}

	var cancels []context.CancelFunc
	var wg sync.WaitGroup
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
		wg.Wait()
	}()

	for name, path := range s.Kubeconfigs {
		cfg, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return fmt.Errorf("loading kubeconfig %s: %w", path, err)
		}
		cfg.QPS = qps
		cfg.Burst = burst

		cl, err := cluster.New(cfg)
		if err != nil {
			return fmt.Errorf("creating cluster client for %s: %w", name, err)
		}

		clusterCtx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)

		// Start the cluster cache; required for cluster.Cluster to be usable.
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			if err := cl.Start(clusterCtx); err != nil {
				logger.Error(err, "cluster runtime stopped", "cluster", name)
			}
		}(name)

		if !cl.GetCache().WaitForCacheSync(clusterCtx) {
			cancel()
			return fmt.Errorf("cache sync failed for cluster %s", name)
		}

		if err := aware.Engage(clusterCtx, multicluster.ClusterName(name), cl); err != nil {
			cancel()
			return fmt.Errorf("engaging cluster %s: %w", name, err)
		}
		logger.Info("cluster engaged", "cluster", name)
	}

	<-ctx.Done()
	return nil
}
