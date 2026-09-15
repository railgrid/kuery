// Command kuery-kcp runs the kuery API server against kcp, indexing objects
// across workspaces discovered via an APIExport virtual workspace.
//
// It shares the entire server core with cmd/kuery and differs only in the
// cluster source (the kcp apiexport provider) and, in full mode, the
// ConfigResolver used to reach each workspace's front-proxy endpoint.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/railgrid/kuery/pkg/app"
	providerkcp "github.com/railgrid/kuery/pkg/provider/kcp"
	kuerysync "github.com/railgrid/kuery/pkg/sync"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/cli"
)

// Options holds the configuration for the kuery-kcp server.
type Options struct {
	SecureServing *options.SecureServingOptionsWithLoopback

	StoreDriver string
	StoreDSN    string

	SyncBlacklist string
	SyncWhitelist string

	// KCPKubeconfig points at the kcp shard / control plane serving the
	// APIExportEndpointSlice (typically the APIExport's own workspace).
	KCPKubeconfig string
	// APIExportEndpointSlice is the APIExportEndpointSlice object to watch.
	APIExportEndpointSlice string
	// Mode selects indexing depth: "scoped" (APIExport's types only, via the
	// virtual workspace) or "full" (every type per workspace, via the
	// front-proxy endpoint using AdminKubeconfig).
	Mode string
	// AdminKubeconfig drives the per-workspace clients in full mode. Its host is
	// the front-proxy base; kuery appends /clusters/<name> per workspace.
	AdminKubeconfig string
}

const (
	modeScoped = "scoped"
	modeFull   = "full"
)

// NewOptions creates default options.
func NewOptions() *Options {
	o := &Options{
		SecureServing: options.NewSecureServingOptions().WithLoopback(),
		StoreDriver:   "sqlite",
		StoreDSN:      "kuery.db",
		SyncBlacklist: "secrets,events,events.events.k8s.io",
		Mode:          modeScoped,
	}
	o.SecureServing.BindPort = 6443
	return o
}

// AddFlags adds flags to the flagset.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	fs.StringVar(&o.StoreDriver, "store-driver", o.StoreDriver, "Database driver: sqlite or postgres")
	fs.StringVar(&o.StoreDSN, "store-dsn", o.StoreDSN, "Database connection string")
	fs.StringVar(&o.SyncBlacklist, "sync-blacklist", o.SyncBlacklist, "Comma-separated resources to skip syncing (default: secrets,events,events.events.k8s.io). Empty string syncs everything.")
	fs.StringVar(&o.SyncWhitelist, "sync-whitelist", o.SyncWhitelist, "Comma-separated resources to sync exclusively (resource or resource.group). Empty syncs everything watchable; the blacklist still applies.")
	fs.StringVar(&o.KCPKubeconfig, "kcp-kubeconfig", o.KCPKubeconfig, "Path to a kubeconfig for the kcp workspace serving the APIExportEndpointSlice")
	fs.StringVar(&o.APIExportEndpointSlice, "kcp-apiexport-endpointslice", o.APIExportEndpointSlice, "Name of the APIExportEndpointSlice to watch for workspaces")
	fs.StringVar(&o.Mode, "kcp-mode", o.Mode, "Indexing depth: 'scoped' (APIExport's types via the virtual workspace) or 'full' (every type per workspace via the front-proxy)")
	fs.StringVar(&o.AdminKubeconfig, "admin-kubeconfig", o.AdminKubeconfig, "Path to an admin kubeconfig used to index workspaces in full mode (host is the front-proxy base)")
}

// Validate checks option values for validity.
func (o *Options) Validate() error {
	switch o.StoreDriver {
	case "sqlite", "postgres":
	default:
		return fmt.Errorf("unsupported store driver: %s", o.StoreDriver)
	}
	if o.KCPKubeconfig == "" {
		return fmt.Errorf("--kcp-kubeconfig is required")
	}
	if o.APIExportEndpointSlice == "" {
		return fmt.Errorf("--kcp-apiexport-endpointslice is required")
	}
	switch o.Mode {
	case modeScoped:
	case modeFull:
		if o.AdminKubeconfig == "" {
			return fmt.Errorf("--admin-kubeconfig is required when --kcp-mode=full")
		}
	default:
		return fmt.Errorf("unsupported --kcp-mode: %s (want %q or %q)", o.Mode, modeScoped, modeFull)
	}
	return nil
}

// Run starts the kuery-kcp API server.
func (o *Options) Run(ctx context.Context) error {
	kcpConfig, err := clientcmd.BuildConfigFromFlags("", o.KCPKubeconfig)
	if err != nil {
		return fmt.Errorf("loading kcp kubeconfig: %w", err)
	}

	source, err := providerkcp.NewSource(providerkcp.Options{
		RestConfig:        kcpConfig,
		EndpointSliceName: o.APIExportEndpointSlice,
	})
	if err != nil {
		return err
	}

	var resolver kuerysync.ConfigResolver
	if o.Mode == modeFull {
		adminConfig, err := clientcmd.BuildConfigFromFlags("", o.AdminKubeconfig)
		if err != nil {
			return fmt.Errorf("loading admin kubeconfig: %w", err)
		}
		adminConfig.QPS = 100
		adminConfig.Burst = 200
		resolver = providerkcp.FullModeConfigResolver(adminConfig)
	}

	cfg := &app.Config{
		StoreDriver:    o.StoreDriver,
		StoreDSN:       o.StoreDSN,
		SecureServing:  o.SecureServing,
		Blacklist:      kuerysync.NewBlacklist(parseGVRList(o.SyncBlacklist)),
		Whitelist:      buildWhitelist(o.SyncWhitelist),
		ConfigResolver: resolver,
		Source:         source,
	}
	return cfg.Run(ctx)
}

// buildWhitelist returns nil (sync everything) for an empty flag.
func buildWhitelist(raw string) *kuerysync.Whitelist {
	gvrs := parseGVRList(raw)
	if gvrs == nil {
		return nil
	}
	return kuerysync.NewWhitelist(gvrs)
}

// parseGVRList parses a comma-separated "resource" / "resource.group" list.
func parseGVRList(raw string) []schema.GroupVersionResource {
	var gvrs []schema.GroupVersionResource
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ".", 2)
		group := ""
		if len(parts) == 2 {
			group = parts[1]
		}
		gvrs = append(gvrs, schema.GroupVersionResource{Group: group, Resource: parts[0]})
	}
	return gvrs
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	o := NewOptions()
	cmd := &cobra.Command{
		Use:   "kuery-kcp",
		Short: "Kubernetes query API server for kcp workspaces",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(ctx)
		},
	}
	o.AddFlags(cmd.Flags())

	code := cli.Run(cmd)
	os.Exit(code)
}
