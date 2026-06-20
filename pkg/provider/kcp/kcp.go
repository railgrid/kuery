// Package kcp wires the kcp APIExport virtual-workspace provider as a
// provider.Source, so kuery can index objects across kcp workspaces.
//
// The apiexport provider watches an APIExportEndpointSlice and engages each
// workspace (logicalcluster) that binds the APIExport as a cluster, calling
// Engage/(ctx-cancel) on the SyncController like any other provider.
//
// Two indexing depths are supported via the SyncController's ConfigResolver:
//
//   - scoped (default): index against the cluster the provider hands out, i.e.
//     the virtual-workspace endpoint. This only exposes the APIExport's exported
//     and permission-claimed types, but scales to many workspaces cheaply.
//   - full: index every resource type in each workspace by addressing the
//     workspace's real front-proxy endpoint (<admin-host>/clusters/<name>) with
//     admin credentials. Use FullModeConfigResolver for this.
package kcp

import (
	"fmt"
	"net/url"

	"github.com/faroshq/kuery/pkg/provider"
	kuerysync "github.com/faroshq/kuery/pkg/sync"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/kcp-dev/multicluster-provider/apiexport"

	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
)

// providerScheme knows the kcp API types the apiexport provider needs to watch
// (APIExportEndpointSlice, APIBinding) plus the standard client-go types.
func providerScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(scheme.AddToScheme(s))
	utilruntime.Must(apisv1alpha1.AddToScheme(s))
	utilruntime.Must(corev1alpha1.AddToScheme(s))
	utilruntime.Must(tenancyv1alpha1.AddToScheme(s))
	return s
}

// Options configures the kcp apiexport Source.
type Options struct {
	// RestConfig points at the kcp shard / control plane that serves the
	// APIExportEndpointSlice (typically the APIExport's own workspace).
	RestConfig *rest.Config
	// EndpointSliceName is the APIExportEndpointSlice object to watch.
	EndpointSliceName string
}

// NewSource builds a provider.Source that engages each workspace binding the
// APIExport as a cluster.
func NewSource(opts Options) (provider.Source, error) {
	if opts.RestConfig == nil {
		return nil, fmt.Errorf("kcp: RestConfig is required")
	}
	if opts.EndpointSliceName == "" {
		return nil, fmt.Errorf("kcp: EndpointSliceName is required")
	}

	p, err := apiexport.New(opts.RestConfig, opts.EndpointSliceName, apiexport.Options{
		Scheme: providerScheme(),
	})
	if err != nil {
		return nil, fmt.Errorf("kcp: building apiexport provider: %w", err)
	}
	return provider.FromProviderRunnable(p), nil
}

// FullModeConfigResolver returns a kuerysync.ConfigResolver that indexes every
// resource type in each workspace by addressing the workspace's front-proxy
// endpoint (<adminConfig.Host>/clusters/<name>) with the admin credentials,
// instead of the scoped virtual-workspace endpoint the provider hands out.
func FullModeConfigResolver(adminConfig *rest.Config) kuerysync.ConfigResolver {
	return func(name multicluster.ClusterName, _ cluster.Cluster) (*rest.Config, error) {
		host, err := url.JoinPath(adminConfig.Host, "clusters", name.String())
		if err != nil {
			return nil, fmt.Errorf("kcp: building workspace host for %s: %w", name, err)
		}
		cfg := rest.CopyConfig(adminConfig)
		cfg.Host = host
		return cfg, nil
	}
}
