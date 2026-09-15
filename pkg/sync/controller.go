package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/railgrid/kuery/pkg/store"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// ConfigResolver derives the rest.Config used to index a cluster (discovery +
// dynamic informers) from the engaged multicluster.Cluster. It is the seam that
// keeps the SyncController provider-agnostic: the default returns the cluster's
// own config, while e.g. a kcp "full" mode resolver rewrites the host to a
// front-proxy /clusters/<name> endpoint built from an admin kubeconfig.
type ConfigResolver func(clusterName multicluster.ClusterName, cl cluster.Cluster) (*rest.Config, error)

// defaultConfigResolver indexes against the cluster's own rest.Config.
func defaultConfigResolver(_ multicluster.ClusterName, cl cluster.Cluster) (*rest.Config, error) {
	return cl.GetConfig(), nil
}

// DefaultResyncPeriod is the default informer resync interval.
const DefaultResyncPeriod = 10 * time.Minute

// Config holds configuration for the SyncController.
type Config struct {
	Store     store.Store
	Blacklist *Blacklist
	// Whitelist, when non-nil, restricts which resources are synced (the
	// blacklist still applies on top). Nil syncs everything watchable.
	// Non-whitelisted types are still recorded in resource_types.
	Whitelist    *Whitelist
	ResyncPeriod time.Duration

	// ConfigResolver derives the rest.Config used to index each engaged cluster.
	// Nil defaults to the cluster's own config (cl.GetConfig()).
	ConfigResolver ConfigResolver
}

// SyncController manages per-cluster informers that sync Kubernetes objects into
// the store. It implements the multicluster-runtime multicluster.Aware interface:
// providers (static kubeconfigs, kcp apiexport, Cluster API, ...) call Engage as
// clusters appear and cancel the per-cluster context to disengage them.
type SyncController struct {
	config         Config
	configResolver ConfigResolver

	mu       sync.Mutex
	clusters map[multicluster.ClusterName]*clusterState
}

var _ multicluster.Aware = &SyncController{}

// clusterState tracks the engaged cluster instance and cancel function.
type clusterState struct {
	cluster cluster.Cluster
	cancel  context.CancelFunc
}

// NewSyncController creates a new SyncController.
func NewSyncController(cfg Config) *SyncController {
	if cfg.ResyncPeriod == 0 {
		cfg.ResyncPeriod = DefaultResyncPeriod
	}
	if cfg.Blacklist == nil {
		cfg.Blacklist = NewBlacklist(DefaultBlacklist)
	}
	resolver := cfg.ConfigResolver
	if resolver == nil {
		resolver = defaultConfigResolver
	}
	return &SyncController{
		config:         cfg,
		configResolver: resolver,
		clusters:       make(map[multicluster.ClusterName]*clusterState),
	}
}

// Engage is called by a multicluster provider when a cluster becomes available.
// It runs discovery, populates resource_types, starts informers for all watchable
// resources, and marks the cluster as active. The passed context is tied to the
// cluster's lifecycle: when the provider removes the cluster it cancels ctx, which
// stops the informers and marks the cluster stale. Engage is re-entrant and
// non-blocking, and a no-op when re-called with the same cluster instance.
func (sc *SyncController) Engage(ctx context.Context, clusterName multicluster.ClusterName, cl cluster.Cluster) error {
	name := clusterName.String()
	logger := klog.FromContext(ctx).WithValues("cluster", name)

	sc.mu.Lock()
	// No-op if the same cluster instance is already engaged.
	if existing, ok := sc.clusters[clusterName]; ok && existing.cluster == cl {
		sc.mu.Unlock()
		return nil
	}
	// A different instance was engaged before: cancel it before replacing.
	if existing, ok := sc.clusters[clusterName]; ok && existing.cancel != nil {
		existing.cancel()
	}
	clusterCtx, clusterCancel := context.WithCancel(ctx)
	sc.clusters[clusterName] = &clusterState{cluster: cl, cancel: clusterCancel}
	sc.mu.Unlock()

	logger.Info("engaging cluster")

	// Mark cluster as active.
	now := time.Now()
	if err := sc.config.Store.UpsertCluster(ctx, &store.ClusterModel{
		Name:      name,
		Status:    "active",
		LastSeen:  now,
		EngagedAt: &now,
		TTL:       3600,
	}); err != nil {
		clusterCancel()
		return fmt.Errorf("failed to upsert cluster %s: %w", name, err)
	}

	// Resolve the rest.Config used to index this cluster. The default is the
	// cluster's own config; kcp "full" mode rewrites it to an admin endpoint.
	restConfig, err := sc.configResolver(clusterName, cl)
	if err != nil {
		clusterCancel()
		return fmt.Errorf("failed to resolve config for cluster %s: %w", name, err)
	}

	// Create discovery client.
	dc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		clusterCancel()
		return fmt.Errorf("failed to create discovery client for %s: %w", name, err)
	}

	// Run discovery to populate resource_types and get watchable resources.
	watchable, err := RunDiscovery(clusterCtx, name, dc, sc.config.Store, sc.config.Blacklist, sc.config.Whitelist)
	if err != nil {
		clusterCancel()
		return fmt.Errorf("discovery failed for cluster %s: %w", name, err)
	}

	// Create dynamic client for informers.
	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		clusterCancel()
		return fmt.Errorf("failed to create dynamic client for %s: %w", name, err)
	}

	// Start informers for all watchable resources.
	go sc.runInformers(clusterCtx, name, dynClient, watchable)

	// Watch CRDs for discovery refresh.
	go sc.watchCRDs(clusterCtx, name, restConfig, dc)

	// On disengage (provider cancels ctx, or explicit Disengage), mark stale.
	go func() {
		<-clusterCtx.Done()
		sc.mu.Lock()
		if existing, ok := sc.clusters[clusterName]; ok && existing.cluster == cl {
			delete(sc.clusters, clusterName)
		}
		sc.mu.Unlock()
		sc.markStale(context.WithoutCancel(clusterCtx), name)
	}()

	logger.Info("cluster engaged", "watchable", len(watchable))
	return nil
}

// Disengage stops a cluster's informers and marks it stale. Providers normally
// disengage by cancelling the per-cluster context passed to Engage; this method
// supports explicit removal and graceful shutdown of a named cluster.
func (sc *SyncController) Disengage(ctx context.Context, clusterName string) error {
	logger := klog.FromContext(ctx).WithValues("cluster", clusterName)
	logger.Info("disengaging cluster")

	sc.mu.Lock()
	if state, ok := sc.clusters[multicluster.ClusterName(clusterName)]; ok {
		if state.cancel != nil {
			state.cancel()
		}
		delete(sc.clusters, multicluster.ClusterName(clusterName))
	}
	sc.mu.Unlock()

	if err := sc.markStale(ctx, clusterName); err != nil {
		logger.Error(err, "failed to mark cluster as stale")
		return err
	}

	logger.Info("cluster disengaged")
	return nil
}

// markStale records a cluster as no longer engaged.
func (sc *SyncController) markStale(ctx context.Context, clusterName string) error {
	now := time.Now()
	return sc.config.Store.UpsertCluster(ctx, &store.ClusterModel{
		Name:     clusterName,
		Status:   "stale",
		LastSeen: now,
	})
}

// runInformers starts a dynamic shared informer factory and adds event handlers
// for all watchable resources.
func (sc *SyncController) runInformers(ctx context.Context, clusterName string, dynClient dynamic.Interface, watchable []DiscoveredResource) {
	logger := klog.FromContext(ctx).WithValues("cluster", clusterName)

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, sc.config.ResyncPeriod)

	for _, res := range watchable {
		informer := factory.ForResource(res.GVR).Informer()

		handler := &EventHandler{
			Store:       sc.config.Store,
			ClusterName: clusterName,
			GVR:         res.GVR,
			Kind:        res.Kind,
		}

		if _, err := informer.AddEventHandler(handler); err != nil {
			logger.Error(err, "failed to add event handler", "resource", res.GVR.String())
			continue
		}
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	logger.Info("all informers synced", "count", len(watchable))

	// Block until context is cancelled.
	<-ctx.Done()
	logger.Info("informers stopped")
}

// watchCRDs watches CustomResourceDefinition changes and triggers discovery refresh.
func (sc *SyncController) watchCRDs(ctx context.Context, clusterName string, restConfig *rest.Config, dc discovery.DiscoveryInterface) {
	logger := klog.FromContext(ctx).WithValues("cluster", clusterName)

	// Watch CRDs using a dynamic informer.
	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		logger.Error(err, "failed to create dynamic client for CRD watch")
		return
	}

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 5*time.Minute, "", nil)
	informer := factory.ForResource(crdGVR).Informer()

	// On CRD changes, re-run discovery. Debounce by only refreshing every 30s.
	var lastRefresh time.Time
	refreshHandler := &crdRefreshHandler{
		refresh: func() {
			now := time.Now()
			if now.Sub(lastRefresh) < 30*time.Second {
				return
			}
			lastRefresh = now
			logger.Info("CRD change detected, refreshing discovery")
			if _, err := RunDiscovery(ctx, clusterName, dc, sc.config.Store, sc.config.Blacklist, sc.config.Whitelist); err != nil {
				logger.Error(err, "discovery refresh failed")
			}
		},
	}

	if _, err := informer.AddEventHandler(refreshHandler); err != nil {
		logger.Error(err, "failed to add CRD watch handler")
		return
	}

	factory.Start(ctx.Done())
	<-ctx.Done()
}

// crdRefreshHandler triggers a callback on any CRD change.
type crdRefreshHandler struct {
	refresh func()
}

func (h *crdRefreshHandler) OnAdd(_ interface{}, _ bool) { h.refresh() }
func (h *crdRefreshHandler) OnUpdate(_, _ interface{})   { h.refresh() }
func (h *crdRefreshHandler) OnDelete(_ interface{})      { h.refresh() }
