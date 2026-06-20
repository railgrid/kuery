package sync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/faroshq/kuery/pkg/store"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// newAwareTestStore returns a test store pinned to a single DB connection.
// These tests engage clusters concurrently; an in-memory SQLite DSN hands each
// pooled connection its own database, so without pinning, a write on one
// connection isn't visible on another. Production uses file/postgres DSNs.
func newAwareTestStore(t *testing.T) store.Store {
	t.Helper()
	s := newTestStore(t)
	sqlDB, err := s.RawDB().DB()
	if err != nil {
		t.Fatalf("get sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	return s
}

// fakeCluster is a minimal cluster.Cluster whose only meaningful method is
// GetConfig. It lets us exercise the SyncController's Aware lifecycle without a
// real Kubernetes cluster.
type fakeCluster struct {
	cfg *rest.Config
}

var _ cluster.Cluster = &fakeCluster{}

func (f *fakeCluster) GetConfig() *rest.Config                              { return f.cfg }
func (f *fakeCluster) GetHTTPClient() *http.Client                         { return nil }
func (f *fakeCluster) GetCache() cache.Cache                               { return nil }
func (f *fakeCluster) GetScheme() *runtime.Scheme                          { return runtime.NewScheme() }
func (f *fakeCluster) GetClient() client.Client                            { return nil }
func (f *fakeCluster) GetFieldIndexer() client.FieldIndexer                { return nil }
func (f *fakeCluster) GetRESTMapper() meta.RESTMapper                      { return nil }
func (f *fakeCluster) GetAPIReader() client.Reader                         { return nil }
func (f *fakeCluster) GetEventRecorderFor(_ string) record.EventRecorder   { return nil }
func (f *fakeCluster) GetEventRecorder(_ string) events.EventRecorder      { return nil }
func (f *fakeCluster) Start(_ context.Context) error                       { return nil }

// emptyDiscoveryServer serves a minimal, valid discovery surface with no
// resources, so RunDiscovery succeeds and yields nothing watchable. Any other
// request returns an empty list (keeps the CRD informer quiet).
func emptyDiscoveryServer(t *testing.T) *httptest.Server {
	t.Helper()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, metav1.APIVersions{Versions: []string{}})
	})
	mux.HandleFunc("/apis", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, metav1.APIGroupList{})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Hold watch requests open quietly instead of returning a body the
		// reflector can't decode (which would hot-loop and starve CPU).
		if r.URL.Query().Get("watch") == "true" {
			<-r.Context().Done()
			return
		}
		writeJSON(w, map[string]any{
			"apiVersion": "v1", "kind": "List",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": []any{},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestEngage_MarksActiveThenStaleOnContextCancel(t *testing.T) {
	s := newAwareTestStore(t)
	srv := emptyDiscoveryServer(t)
	sc := NewSyncController(Config{Store: s})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cl := &fakeCluster{cfg: &rest.Config{Host: srv.URL}}
	if err := sc.Engage(ctx, multicluster.ClusterName("ws-1"), cl); err != nil {
		t.Fatalf("Engage failed: %v", err)
	}

	c, err := s.GetCluster(ctx, "ws-1")
	if err != nil {
		t.Fatalf("GetCluster failed: %v", err)
	}
	if c.Status != "active" {
		t.Fatalf("Status = %q, want active", c.Status)
	}

	// Cancelling the per-cluster context (what a provider does on removal) must
	// mark the cluster stale.
	cancel()
	if err := waitFor(2*time.Second, func() bool {
		c, err := s.GetCluster(context.Background(), "ws-1")
		return err == nil && c.Status == "stale"
	}); err != nil {
		t.Fatalf("cluster not marked stale after context cancel: %v", err)
	}
}

func TestEngage_UsesConfigResolver(t *testing.T) {
	s := newAwareTestStore(t)
	srv := emptyDiscoveryServer(t)

	var gotName multicluster.ClusterName
	var gotCluster cluster.Cluster
	resolver := func(name multicluster.ClusterName, cl cluster.Cluster) (*rest.Config, error) {
		gotName = name
		gotCluster = cl
		return &rest.Config{Host: srv.URL}, nil
	}
	sc := NewSyncController(Config{Store: s, ConfigResolver: resolver})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cl := &fakeCluster{cfg: &rest.Config{Host: "https://unused-by-resolver"}}
	if err := sc.Engage(ctx, multicluster.ClusterName("ws-7"), cl); err != nil {
		t.Fatalf("Engage failed: %v", err)
	}
	if gotName != "ws-7" {
		t.Errorf("resolver got name %q, want ws-7", gotName)
	}
	if gotCluster != cl {
		t.Errorf("resolver got wrong cluster instance")
	}
}

func TestEngage_ResolverErrorFailsFast(t *testing.T) {
	s := newAwareTestStore(t)
	resolver := func(multicluster.ClusterName, cluster.Cluster) (*rest.Config, error) {
		return nil, context.DeadlineExceeded
	}
	sc := NewSyncController(Config{Store: s, ConfigResolver: resolver})

	cl := &fakeCluster{cfg: &rest.Config{Host: "https://x"}}
	err := sc.Engage(context.Background(), multicluster.ClusterName("ws-9"), cl)
	if err == nil {
		t.Fatal("expected Engage to fail when resolver errors")
	}
}

func TestEngage_NoOpOnSameInstance(t *testing.T) {
	s := newAwareTestStore(t)
	srv := emptyDiscoveryServer(t)

	var calls int
	resolver := func(multicluster.ClusterName, cluster.Cluster) (*rest.Config, error) {
		calls++
		return &rest.Config{Host: srv.URL}, nil
	}
	sc := NewSyncController(Config{Store: s, ConfigResolver: resolver})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cl := &fakeCluster{cfg: &rest.Config{Host: srv.URL}}
	if err := sc.Engage(ctx, multicluster.ClusterName("ws-1"), cl); err != nil {
		t.Fatalf("first Engage failed: %v", err)
	}
	// Re-engaging the same instance is a no-op: the resolver must not run again.
	if err := sc.Engage(ctx, multicluster.ClusterName("ws-1"), cl); err != nil {
		t.Fatalf("second Engage failed: %v", err)
	}
	if calls != 1 {
		t.Errorf("resolver called %d times, want 1 (second Engage should be a no-op)", calls)
	}
}

// TestMultiCluster_EngageAndPerClusterDisengage engages several clusters through
// the Aware interface (as a multicluster provider would) and verifies that
// cancelling one cluster's context disengages only that cluster.
func TestMultiCluster_EngageAndPerClusterDisengage(t *testing.T) {
	s := newAwareTestStore(t)
	srv := emptyDiscoveryServer(t)
	sc := NewSyncController(Config{Store: s})

	base, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()

	names := []string{"ws-a", "ws-b", "ws-c"}
	cancels := make(map[string]context.CancelFunc, len(names))
	for _, name := range names {
		cctx, ccancel := context.WithCancel(base)
		cancels[name] = ccancel
		cl := &fakeCluster{cfg: &rest.Config{Host: srv.URL}}
		if err := sc.Engage(cctx, multicluster.ClusterName(name), cl); err != nil {
			t.Fatalf("Engage %s failed: %v", name, err)
		}
	}

	for _, name := range names {
		c, err := s.GetCluster(base, name)
		if err != nil || c.Status != "active" {
			t.Fatalf("cluster %s: status=%v err=%v, want active", name, c, err)
		}
	}

	// Disengage only ws-b.
	cancels["ws-b"]()
	if err := waitFor(2*time.Second, func() bool {
		c, err := s.GetCluster(context.Background(), "ws-b")
		return err == nil && c.Status == "stale"
	}); err != nil {
		t.Fatalf("ws-b not stale after its context was cancelled: %v", err)
	}

	// ws-a and ws-c must remain active.
	for _, name := range []string{"ws-a", "ws-c"} {
		c, err := s.GetCluster(context.Background(), name)
		if err != nil || c.Status != "active" {
			t.Errorf("cluster %s: status=%v err=%v, want still active", name, c, err)
		}
	}
}

func waitFor(timeout time.Duration, fn func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
