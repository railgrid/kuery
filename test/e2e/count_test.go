//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestCount_WithLimit(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}},
		},
		Limit: 2,
		Count: true,
	})
	if status.Count == nil {
		t.Fatal("expected count in response")
	}
	if *status.Count <= 2 {
		t.Fatalf("expected count > 2 (more objects than limit), got %d", *status.Count)
	}
	if len(status.Objects) != 2 {
		t.Fatalf("expected 2 objects (limited), got %d", len(status.Objects))
	}
}

func TestCount_WithFilter(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Count: true,
	})
	if status.Count == nil {
		t.Fatal("expected count")
	}
	// Should be exactly 2 Deployments in demo: nginx + redis (across 2 clusters).
	if *status.Count != 2 {
		t.Fatalf("expected count=2 Deployments in demo, got %d", *status.Count)
	}
}

func TestCount_Zero(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "nonexistent-object-xyz"}},
		},
		Count: true,
	})
	if status.Count == nil || *status.Count != 0 {
		t.Fatalf("expected count=0, got %v", status.Count)
	}
}
