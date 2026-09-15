package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/railgrid/kuery/pkg/app"
	"github.com/railgrid/kuery/pkg/provider"
	"github.com/railgrid/kuery/pkg/provider/static"
	kuerysync "github.com/railgrid/kuery/pkg/sync"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/component-base/cli"
)

// Options holds the configuration for the kuery server.
type Options struct {
	SecureServing *options.SecureServingOptionsWithLoopback

	StoreDriver string
	StoreDSN    string

	// SyncEnabled enables the sync controller to watch clusters.
	SyncEnabled bool

	// Kubeconfigs is a list of name=path pairs for clusters to sync.
	// Example: "cluster-a=/path/to/a.kubeconfig,cluster-b=/path/to/b.kubeconfig"
	Kubeconfigs string

	// SyncBlacklist overrides the default blacklist. Comma-separated resource names.
	// Default: "secrets,events,events.events.k8s.io"
	// Set to empty string to sync everything.
	SyncBlacklist string
	SyncWhitelist string
}

// NewOptions creates default options.
func NewOptions() *Options {
	o := &Options{
		SecureServing: options.NewSecureServingOptions().WithLoopback(),
		StoreDriver:   "sqlite",
		StoreDSN:      "kuery.db",
		SyncBlacklist: "secrets,events,events.events.k8s.io",
		SyncWhitelist: "",
	}
	o.SecureServing.BindPort = 6443
	return o
}

// AddFlags adds flags to the flagset.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	fs.StringVar(&o.StoreDriver, "store-driver", o.StoreDriver, "Database driver: sqlite or postgres")
	fs.StringVar(&o.StoreDSN, "store-dsn", o.StoreDSN, "Database connection string")
	fs.BoolVar(&o.SyncEnabled, "sync-enabled", o.SyncEnabled, "Enable sync controller to watch clusters")
	fs.StringVar(&o.Kubeconfigs, "kubeconfigs", o.Kubeconfigs, "Comma-separated list of name=path pairs for clusters to sync (e.g. cluster-a=/path/a.kubeconfig,cluster-b=/path/b.kubeconfig)")
	fs.StringVar(&o.SyncBlacklist, "sync-blacklist", o.SyncBlacklist, "Comma-separated resources to skip syncing (default: secrets,events,events.events.k8s.io). Empty string syncs everything.")
	fs.StringVar(&o.SyncWhitelist, "sync-whitelist", o.SyncWhitelist, "Comma-separated resources to sync exclusively (resource or resource.group). Empty syncs everything watchable; the blacklist still applies. Non-whitelisted types remain discoverable in resource_types.")
}

// Complete fills in fields required to have valid data.
func (o *Options) Complete() error {
	return nil
}

// Validate checks option values for validity.
func (o *Options) Validate() error {
	switch o.StoreDriver {
	case "sqlite", "postgres":
	default:
		return fmt.Errorf("unsupported store driver: %s", o.StoreDriver)
	}
	return nil
}

// parseKubeconfigs parses the --kubeconfigs flag into a map of name -> path.
func (o *Options) parseKubeconfigs() (map[string]string, error) {
	if o.Kubeconfigs == "" {
		return nil, nil
	}
	result := make(map[string]string)
	for _, entry := range strings.Split(o.Kubeconfigs, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid kubeconfig entry %q: expected name=path", entry)
		}
		name := strings.TrimSpace(parts[0])
		path := strings.TrimSpace(parts[1])
		if name == "" || path == "" {
			return nil, fmt.Errorf("invalid kubeconfig entry %q: name and path must be non-empty", entry)
		}
		result[name] = path
	}
	return result, nil
}

// buildBlacklist creates a Blacklist from the --sync-blacklist flag.
func (o *Options) buildBlacklist() *kuerysync.Blacklist {
	return kuerysync.NewBlacklist(parseGVRList(o.SyncBlacklist))
}

// buildWhitelist creates a Whitelist from the --sync-whitelist flag.
// Empty flag means nil (sync everything watchable).
func (o *Options) buildWhitelist() *kuerysync.Whitelist {
	gvrs := parseGVRList(o.SyncWhitelist)
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

// Run starts the kuery API server with a static-kubeconfig cluster source.
func (o *Options) Run(ctx context.Context) error {
	kubeconfigs, err := o.parseKubeconfigs()
	if err != nil {
		return fmt.Errorf("failed to parse kubeconfigs: %w", err)
	}

	var source provider.Source
	if len(kubeconfigs) > 0 {
		source = &static.Source{Kubeconfigs: kubeconfigs}
	}

	cfg := &app.Config{
		StoreDriver:   o.StoreDriver,
		StoreDSN:      o.StoreDSN,
		SecureServing: o.SecureServing,
		Blacklist:     o.buildBlacklist(),
		Whitelist:     o.buildWhitelist(),
		Source:        source,
	}
	return cfg.Run(ctx)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	o := NewOptions()
	cmd := &cobra.Command{
		Use:   "kuery",
		Short: "Kubernetes query API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
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
