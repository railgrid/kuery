// Package provider defines the portability seam between kuery's indexing core
// and the source of clusters to index. A Source discovers clusters and engages
// them on a multicluster.Aware (the SyncController) as they appear, cancelling
// the per-cluster context to disengage them.
//
// This mirrors sigs.k8s.io/multicluster-runtime's ProviderRunnable.Start, so any
// multicluster-runtime provider (kcp apiexport, Cluster API, kubeconfig secrets,
// ...) is a Source with at most a trivial adapter, and kuery's static
// kubeconfig list is just another Source.
package provider

import (
	"context"

	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// Source discovers clusters and drives their lifecycle on the given Aware.
//
// Run must block until ctx is done. For each discovered cluster it calls
// aware.Engage with a context tied to that cluster's lifecycle, and cancels that
// context when the cluster goes away. Run is the only coupling each multicluster
// backend has to kuery.
type Source interface {
	Run(ctx context.Context, aware multicluster.Aware) error
}

// RunnableFunc adapts a bare function to a Source.
type RunnableFunc func(ctx context.Context, aware multicluster.Aware) error

// Run implements Source.
func (f RunnableFunc) Run(ctx context.Context, aware multicluster.Aware) error {
	return f(ctx, aware)
}

// FromProviderRunnable adapts any multicluster-runtime ProviderRunnable (e.g. the
// kcp apiexport provider) to a Source, since both expose Start(ctx, Aware).
func FromProviderRunnable(p multicluster.ProviderRunnable) Source {
	return RunnableFunc(p.Start)
}
