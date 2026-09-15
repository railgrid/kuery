//go:build e2e

package e2e_test

import (
	"sort"
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestOrdering_NameAsc(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	names := objectNames(t, status.Objects)
	if !sort.StringsAreSorted(names) {
		t.Fatalf("expected alphabetical order, got %v", names)
	}
}

func TestOrdering_NameDesc(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Order:   []v1alpha1.OrderSpec{{Field: "name", Direction: v1alpha1.SortDesc}},
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	names := objectNames(t, status.Objects)
	if len(names) < 2 {
		t.Skip("need at least 2 results to test ordering")
	}
	// Check reverse order.
	reversed := make([]string, len(names))
	copy(reversed, names)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))
	for i, n := range names {
		if n != reversed[i] {
			t.Fatalf("expected reverse alphabetical, got %v", names)
		}
	}
}

func TestOrdering_MultiField(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}},
		},
		Limit: 20,
		Order: []v1alpha1.OrderSpec{
			{Field: "kind", Direction: v1alpha1.SortAsc},
			{Field: "name", Direction: v1alpha1.SortAsc},
		},
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{
			"kind": true, "metadata": map[string]any{"name": true},
		})},
	})
	if len(status.Objects) < 2 {
		t.Skip("need at least 2 results")
	}
	// Verify kinds are in ascending order.
	kinds := objectKinds(t, status.Objects)
	if !sort.StringsAreSorted(kinds) {
		t.Fatalf("expected kinds in ascending order, got %v", kinds)
	}
}
