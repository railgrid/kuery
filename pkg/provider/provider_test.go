package provider

import (
	"context"
	"errors"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// recordingAware records Engage calls.
type recordingAware struct {
	engaged []multicluster.ClusterName
}

func (r *recordingAware) Engage(_ context.Context, name multicluster.ClusterName, _ cluster.Cluster) error {
	r.engaged = append(r.engaged, name)
	return nil
}

func TestRunnableFunc_Run(t *testing.T) {
	called := false
	var gotAware multicluster.Aware
	aware := &recordingAware{}

	src := RunnableFunc(func(_ context.Context, a multicluster.Aware) error {
		called = true
		gotAware = a
		return nil
	})

	if err := src.Run(context.Background(), aware); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !called {
		t.Error("underlying func not called")
	}
	if gotAware != aware {
		t.Error("Aware not passed through")
	}
}

func TestRunnableFunc_PropagatesError(t *testing.T) {
	want := errors.New("boom")
	src := RunnableFunc(func(_ context.Context, _ multicluster.Aware) error { return want })
	if err := src.Run(context.Background(), &recordingAware{}); !errors.Is(err, want) {
		t.Fatalf("Run error = %v, want %v", err, want)
	}
}

// fakeProviderRunnable implements multicluster.ProviderRunnable; its Start engages
// a couple of clusters on the given Aware, like a real provider would.
type fakeProviderRunnable struct {
	names []multicluster.ClusterName
}

func (f *fakeProviderRunnable) Start(ctx context.Context, aware multicluster.Aware) error {
	for _, n := range f.names {
		if err := aware.Engage(ctx, n, nil); err != nil {
			return err
		}
	}
	return nil
}

func TestFromProviderRunnable_DelegatesToStart(t *testing.T) {
	p := &fakeProviderRunnable{names: []multicluster.ClusterName{"ws-a", "ws-b"}}
	src := FromProviderRunnable(p)

	aware := &recordingAware{}
	if err := src.Run(context.Background(), aware); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(aware.engaged) != 2 || aware.engaged[0] != "ws-a" || aware.engaged[1] != "ws-b" {
		t.Fatalf("engaged = %v, want [ws-a ws-b]", aware.engaged)
	}
}
