// Package app holds the shared bootstrap for the kuery API server: store,
// generic apiserver, sync controller, and an optional cluster Source. Each
// binary (cmd/kuery, cmd/kuery-kcp) parses its own flags and differs only in the
// provider.Source and ConfigResolver it supplies here.
package app

import (
	"context"
	"fmt"
	"net/http"

	"github.com/faroshq/kuery/apis/query/v1alpha1"
	"github.com/faroshq/kuery/pkg/provider"
	"github.com/faroshq/kuery/pkg/server"
	"github.com/faroshq/kuery/pkg/store"
	kuerysync "github.com/faroshq/kuery/pkg/sync"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"

	genericapiserver "k8s.io/apiserver/pkg/server"
)

// Config is the fully-resolved configuration for running the kuery server.
type Config struct {
	StoreDriver string
	StoreDSN    string

	SecureServing *options.SecureServingOptionsWithLoopback

	// Sync controller configuration.
	Blacklist      *kuerysync.Blacklist
	Whitelist      *kuerysync.Whitelist
	ConfigResolver kuerysync.ConfigResolver

	// Source engages clusters to sync. Nil runs the query server with no sync.
	Source provider.Source
}

// Run starts the store, sync controller, optional cluster Source, and the
// generic API server, blocking until ctx is cancelled.
func (c *Config) Run(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	// Initialize the store.
	s, err := store.NewStore(store.Config{
		Driver: c.StoreDriver,
		DSN:    c.StoreDSN,
	})
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	defer s.Close()

	if err := s.AutoMigrate(); err != nil {
		return fmt.Errorf("failed to auto-migrate: %w", err)
	}

	// Register effective version for the apiserver component (required by k8s.io/apiserver).
	if compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent) == nil {
		featureGate := utilfeature.DefaultMutableFeatureGate
		effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
		if err := compatibility.DefaultComponentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
			return fmt.Errorf("failed to register effective version: %w", err)
		}
	}

	recommendedConfig := genericapiserver.NewRecommendedConfig(server.Codecs)
	recommendedConfig.EffectiveVersion = compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent)

	// Minimal OpenAPI V3 config (required by SSA/managed fields). Skip installing
	// the OpenAPI handlers (no code-gen), but set the config so the apiserver can
	// create type converters.
	recommendedConfig.SkipOpenAPIInstallation = true
	recommendedConfig.OpenAPIV3Config = &openapicommon.OpenAPIV3Config{
		GetDefinitions: v1alpha1.GetOpenAPIDefinitions,
	}

	if err := c.SecureServing.ApplyTo(&recommendedConfig.SecureServing, &recommendedConfig.LoopbackClientConfig); err != nil {
		return fmt.Errorf("failed to apply secure serving: %w", err)
	}

	// Allow all access in standalone mode. When deployed as an aggregated API
	// server behind kube-apiserver, the parent handles auth.
	recommendedConfig.Authentication.Authenticator = authenticator.RequestFunc(
		func(req *http.Request) (*authenticator.Response, bool, error) {
			return &authenticator.Response{
				User: &user.DefaultInfo{Name: "kuery-user", Groups: []string{"system:masters"}},
			}, true, nil
		})
	recommendedConfig.Authorization.Authorizer = authorizer.AuthorizerFunc(
		func(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
			return authorizer.DecisionAllow, "", nil
		})

	// Create the sync controller.
	syncController := kuerysync.NewSyncController(kuerysync.Config{
		Store:          s,
		Blacklist:      c.Blacklist,
		Whitelist:      c.Whitelist,
		ConfigResolver: c.ConfigResolver,
	})

	// Start the cluster source, if any, to engage clusters into the controller.
	if c.Source != nil {
		go func() {
			if err := c.Source.Run(ctx, syncController); err != nil {
				logger.Error(err, "cluster source stopped")
			}
		}()
	}

	// Build and start the server.
	serverConfig := &server.KueryServerConfig{
		GenericConfig:  recommendedConfig,
		Store:          s,
		SyncController: syncController,
	}

	kueryServer, err := serverConfig.Complete().New()
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	return kueryServer.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
