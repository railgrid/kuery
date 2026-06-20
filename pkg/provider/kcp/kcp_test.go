package kcp

import (
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

func TestFullModeConfigResolver_RewritesHostPerWorkspace(t *testing.T) {
	admin := &rest.Config{
		Host:        "https://front-proxy.example:6443",
		BearerToken: "secret-token",
		TLSClientConfig: rest.TLSClientConfig{
			CAData: []byte("ca-bundle"),
		},
		QPS:   100,
		Burst: 200,
	}
	resolver := FullModeConfigResolver(admin)

	cfg, err := resolver(multicluster.ClusterName("abc123def456"), nil)
	if err != nil {
		t.Fatalf("resolver returned error: %v", err)
	}

	if got, want := cfg.Host, "https://front-proxy.example:6443/clusters/abc123def456"; got != want {
		t.Errorf("Host = %q, want %q", got, want)
	}
	// Credentials and TLS must be carried over so we can read the workspace.
	if cfg.BearerToken != "secret-token" {
		t.Errorf("BearerToken = %q, want carried over", cfg.BearerToken)
	}
	if string(cfg.TLSClientConfig.CAData) != "ca-bundle" {
		t.Errorf("CAData not carried over")
	}
	if cfg.QPS != 100 || cfg.Burst != 200 {
		t.Errorf("QPS/Burst not carried over: %v/%v", cfg.QPS, cfg.Burst)
	}
}

func TestFullModeConfigResolver_DoesNotMutateAdminConfig(t *testing.T) {
	admin := &rest.Config{Host: "https://front-proxy.example:6443"}
	resolver := FullModeConfigResolver(admin)

	if _, err := resolver(multicluster.ClusterName("ws-1"), nil); err != nil {
		t.Fatalf("resolver returned error: %v", err)
	}
	// A second workspace must not inherit the first's path.
	cfg2, err := resolver(multicluster.ClusterName("ws-2"), nil)
	if err != nil {
		t.Fatalf("resolver returned error: %v", err)
	}
	if admin.Host != "https://front-proxy.example:6443" {
		t.Errorf("admin.Host was mutated to %q", admin.Host)
	}
	if got, want := cfg2.Host, "https://front-proxy.example:6443/clusters/ws-2"; got != want {
		t.Errorf("Host = %q, want %q", got, want)
	}
}

func TestFullModeConfigResolver_TrailingSlashHost(t *testing.T) {
	admin := &rest.Config{Host: "https://front-proxy.example:6443/"}
	resolver := FullModeConfigResolver(admin)

	cfg, err := resolver(multicluster.ClusterName("ws-1"), nil)
	if err != nil {
		t.Fatalf("resolver returned error: %v", err)
	}
	if got, want := cfg.Host, "https://front-proxy.example:6443/clusters/ws-1"; got != want {
		t.Errorf("Host = %q, want %q", got, want)
	}
}

func TestNewSource_Validation(t *testing.T) {
	if _, err := NewSource(Options{EndpointSliceName: "es"}); err == nil {
		t.Error("expected error when RestConfig is nil")
	}
	if _, err := NewSource(Options{RestConfig: &rest.Config{Host: "https://x"}}); err == nil {
		t.Error("expected error when EndpointSliceName is empty")
	}
}
