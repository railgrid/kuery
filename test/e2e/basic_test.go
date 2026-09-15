//go:build e2e

package e2e_test

import (
	"testing"
	"time"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBasic_FilterByKind(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	if len(status.Objects) < 2 {
		t.Fatalf("expected at least 2 Deployments (nginx + redis), got %d", len(status.Objects))
	}
	names := objectNames(t, status.Objects)
	if !containsString(names, "nginx") || !containsString(names, "redis") {
		t.Fatalf("expected nginx and redis, got %v", names)
	}
}

func TestBasic_FilterByName(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "nginx", Namespace: "demo"}},
		},
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	for _, obj := range status.Objects {
		name := getProjectedString(t, obj.Object, "metadata", "name")
		if name != "nginx" {
			t.Fatalf("expected all results named 'nginx', got %q", name)
		}
	}
}

func TestBasic_FilterByNamespace(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Namespace: "nonexistent-ns-12345"}},
		},
		Count: true,
	})
	if status.Count == nil || *status.Count != 0 {
		t.Fatalf("expected 0 results for nonexistent namespace, got count=%v", status.Count)
	}
}

func TestBasic_FilterByLabels(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Labels: map[string]string{"app": "nginx"}, Namespace: "demo"},
			},
		},
		Count:   true,
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	if status.Count == nil || *status.Count == 0 {
		t.Fatal("expected results for label app=nginx")
	}
	for _, obj := range status.Objects {
		name := getProjectedString(t, obj.Object, "metadata", "name")
		// All results should be nginx-related objects.
		if name == "redis" || name == "redis-svc" {
			t.Fatalf("label filter should not return redis objects, got %q", name)
		}
	}
}

func TestBasic_FilterByCluster(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	for _, obj := range status.Objects {
		if obj.Cluster != "cluster-a" {
			t.Fatalf("expected all results from cluster-a, got %q", obj.Cluster)
		}
	}
	names := objectNames(t, status.Objects)
	if !containsString(names, "nginx") {
		t.Fatalf("expected nginx in cluster-a, got %v", names)
	}
}

func TestBasic_FilterOR(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Name: "nginx", Namespace: "demo"},
				{Name: "redis", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	names := objectNames(t, status.Objects)
	if !containsString(names, "nginx") || !containsString(names, "redis") {
		t.Fatalf("OR filter should return both nginx and redis, got %v", names)
	}
}

func TestBasic_FilterByConditions(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{
					GroupKind:  &v1alpha1.GroupKindFilter{Kind: "Pod"},
					Namespace:  "demo",
					Conditions: []v1alpha1.ConditionFilter{{Type: "Ready", Status: "True"}},
				},
			},
		},
		Count: true,
	})
	if status.Count == nil || *status.Count == 0 {
		t.Fatal("expected at least 1 Ready Pod in demo namespace")
	}
}

func TestBasic_FilterByTimestamp(t *testing.T) {
	t.Parallel()
	oneHourAgo := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	// After 1 hour ago should return results.
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Namespace: "demo", CreationTimestamp: &v1alpha1.TimestampFilter{After: &oneHourAgo}},
			},
		},
		Count: true,
	})
	if status.Count == nil || *status.Count == 0 {
		t.Fatal("expected results for objects created in last hour")
	}

	// Before 1 hour ago should return 0 (objects were just created).
	status = queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Namespace: "demo", CreationTimestamp: &v1alpha1.TimestampFilter{Before: &oneHourAgo}},
			},
		},
		Count: true,
	})
	if status.Count != nil && *status.Count != 0 {
		t.Fatalf("expected 0 results for objects before 1 hour ago, got %d", *status.Count)
	}
}

func TestBasic_EmptyResult(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "does-not-exist-xyz-12345"}},
		},
	})
	if len(status.Objects) != 0 {
		t.Fatalf("expected 0 results, got %d", len(status.Objects))
	}
	if status.Incomplete {
		t.Fatal("expected incomplete=false for empty result")
	}
}

func TestBasic_LabelExpressionIn(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{
					Namespace: "demo",
					LabelExpressions: []v1alpha1.LabelExpression{
						{Key: "env", Operator: v1alpha1.LabelOpIn, Values: []string{"production", "staging"}},
					},
				},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: projectionSpec(map[string]any{"metadata": map[string]any{"name": true}})},
	})
	names := objectNames(t, status.Objects)
	if !containsString(names, "nginx") || !containsString(names, "redis") {
		t.Fatalf("expected both nginx (production) and redis (staging), got %v", names)
	}
}

func TestBasic_LabelExpressionExists(t *testing.T) {
	t.Parallel()
	// Objects with 'env' label.
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{
					Namespace: "demo",
					LabelExpressions: []v1alpha1.LabelExpression{
						{Key: "env", Operator: v1alpha1.LabelOpExists},
					},
				},
			},
		},
		Count: true,
	})
	existsCount := int64(0)
	if status.Count != nil {
		existsCount = *status.Count
	}

	// Objects without 'env' label.
	status2 := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{
					Namespace: "demo",
					LabelExpressions: []v1alpha1.LabelExpression{
						{Key: "env", Operator: v1alpha1.LabelOpDoesNotExist},
					},
				},
			},
		},
		Count: true,
	})
	notExistsCount := int64(0)
	if status2.Count != nil {
		notExistsCount = *status2.Count
	}

	if existsCount == 0 {
		t.Fatal("expected some objects with env label")
	}
	if notExistsCount == 0 {
		t.Fatal("expected some objects without env label")
	}
}
