//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestPagination_Limit(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter:  &v1alpha1.QueryFilter{Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}}},
		Limit:   3,
		Count:   true,
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	if len(status.Objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(status.Objects))
	}
	if !status.Incomplete {
		t.Fatal("expected incomplete=true when limit is reached")
	}
	if status.Count == nil || *status.Count <= 3 {
		t.Fatalf("expected count > 3, got %v", status.Count)
	}
}

func TestPagination_Offset(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})
	// Page 1.
	page1 := queryKuery(t, v1alpha1.QuerySpec{
		Filter:  &v1alpha1.QueryFilter{Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}}},
		Limit:   2,
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: proj},
	})
	// Page 2 with offset.
	page2 := queryKuery(t, v1alpha1.QuerySpec{
		Filter:  &v1alpha1.QueryFilter{Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}}},
		Limit:   2,
		Page:    &v1alpha1.PageSpec{First: 2},
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: proj},
	})

	if len(page1.Objects) == 0 || len(page2.Objects) == 0 {
		t.Fatal("expected results on both pages")
	}

	// Pages should have different objects (compare cluster+name to handle duplicates across clusters).
	type objKey struct{ cluster, name string }
	keys1 := make(map[objKey]bool)
	for _, o := range page1.Objects {
		name := getProjectedString(t, o.Object, "metadata", "name")
		keys1[objKey{o.Cluster, name}] = true
	}
	for _, o := range page2.Objects {
		name := getProjectedString(t, o.Object, "metadata", "name")
		k := objKey{o.Cluster, name}
		if keys1[k] {
			t.Fatalf("page 1 and page 2 should not overlap, found %v in both", k)
		}
	}
}

func TestPagination_Cursor(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})
	// Page 1 with cursor.
	page1 := queryKuery(t, v1alpha1.QuerySpec{
		Filter:  &v1alpha1.QueryFilter{Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}}},
		Limit:   2,
		Cursor:  true,
		Objects: &v1alpha1.ObjectsSpec{Object: proj},
	})
	if page1.Cursor == nil || page1.Cursor.Next == "" {
		t.Fatal("expected cursor in response")
	}

	// Page 2 using cursor.
	page2 := queryKuery(t, v1alpha1.QuerySpec{
		Filter:  &v1alpha1.QueryFilter{Objects: []v1alpha1.ObjectFilter{{Namespace: "demo"}}},
		Limit:   2,
		Cursor:  true,
		Page:    &v1alpha1.PageSpec{Cursor: page1.Cursor.Next},
		Objects: &v1alpha1.ObjectsSpec{Object: proj},
	})

	if len(page2.Objects) == 0 {
		t.Fatal("expected results on page 2")
	}

	// No duplicates between pages.
	names1 := objectNames(t, page1.Objects)
	names2 := objectNames(t, page2.Objects)
	for _, n := range names1 {
		if containsString(names2, n) {
			t.Fatalf("cursor pagination should not produce duplicates, found %q on both pages", n)
		}
	}
}
