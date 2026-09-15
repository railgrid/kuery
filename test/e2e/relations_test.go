//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestRelation_Descendants(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}
	descendants := status.Objects[0].Relations["descendants"]
	if len(descendants) == 0 {
		t.Fatal("expected at least 1 descendant (ReplicaSet)")
	}
	// First descendant should be a ReplicaSet.
	kind := getProjectedString(t, descendants[0].Object, "kind")
	if kind != "ReplicaSet" {
		t.Fatalf("expected descendant kind=ReplicaSet, got %q", kind)
	}
}

func TestRelation_Owners(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	// First find a ReplicaSet via descendants.
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 || len(status.Objects[0].Relations["descendants"]) == 0 {
		t.Skip("no descendants found to test owners")
	}
	rsName := getProjectedString(t, status.Objects[0].Relations["descendants"][0].Object, "metadata", "name")

	// Now query the RS and ask for owners.
	status2 := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: rsName, Namespace: "demo"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"owners": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status2.Objects) == 0 {
		t.Fatalf("expected RS %s", rsName)
	}
	owners := status2.Objects[0].Relations["owners"]
	if len(owners) == 0 {
		t.Fatal("expected owner (Deployment)")
	}
	ownerName := getProjectedString(t, owners[0].Object, "metadata", "name")
	if ownerName != "nginx" {
		t.Fatalf("expected owner=nginx, got %q", ownerName)
	}
}

func TestRelation_References_Secret(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{Kind: "Pod"}, Namespace: "demo", Labels: map[string]string{"app": "nginx"}},
			},
		},
		Limit: 1,
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"references": {
					Filters: []v1alpha1.ObjectFilter{
						{GroupKind: &v1alpha1.GroupKindFilter{Kind: "Secret"}},
					},
					Objects: &v1alpha1.ObjectsSpec{Object: proj},
				},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected at least 1 nginx Pod")
	}
	refs := status.Objects[0].Relations["references"]
	if !hasName(t, refs, "nginx-tls") {
		t.Fatalf("expected reference to nginx-tls Secret, got %v", objectNames(t, refs))
	}
}

func TestRelation_References_ConfigMap(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{Kind: "Pod"}, Namespace: "demo", Labels: map[string]string{"app": "nginx"}},
			},
		},
		Limit: 1,
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"references": {
					Filters: []v1alpha1.ObjectFilter{
						{GroupKind: &v1alpha1.GroupKindFilter{Kind: "ConfigMap"}},
					},
					Objects: &v1alpha1.ObjectsSpec{Object: proj},
				},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected at least 1 nginx Pod")
	}
	refs := status.Objects[0].Relations["references"]
	if !hasName(t, refs, "nginx-config") {
		t.Fatalf("expected reference to nginx-config ConfigMap, got %v", objectNames(t, refs))
	}
}

func TestRelation_References_PVC(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{Kind: "Pod"}, Namespace: "demo", Labels: map[string]string{"app": "nginx"}},
			},
		},
		Limit: 1,
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"references": {
					Filters: []v1alpha1.ObjectFilter{
						{GroupKind: &v1alpha1.GroupKindFilter{Kind: "PersistentVolumeClaim"}},
					},
					Objects: &v1alpha1.ObjectsSpec{Object: proj},
				},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected at least 1 nginx Pod")
	}
	refs := status.Objects[0].Relations["references"]
	if !hasName(t, refs, "nginx-data") {
		t.Fatalf("expected reference to nginx-data PVC, got %v", objectNames(t, refs))
	}
}

func TestRelation_Selects(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	// Use the Deployment (which has spec.selector.matchLabels) to test selects.
	// The selects relation matches objects whose labels contain the selector's matchLabels.
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"selects": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx Deployment")
	}
	selected := status.Objects[0].Relations["selects"]
	if len(selected) == 0 {
		t.Fatal("expected selects to find objects matching app=nginx labels")
	}
	// Should find Pods (and other objects) with app=nginx labels.
	names := objectNames(t, selected)
	if len(names) == 0 {
		t.Fatal("expected at least 1 selected object")
	}
}

func TestRelation_NestedDescendants(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants": {
					Objects: &v1alpha1.ObjectsSpec{
						Object: proj,
						Relations: map[string]v1alpha1.RelationSpec{
							"descendants": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
						},
					},
				},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}
	// Level 1: ReplicaSet.
	rs := status.Objects[0].Relations["descendants"]
	if len(rs) == 0 {
		t.Fatal("expected ReplicaSet descendants")
	}
	// Level 2: Pods.
	pods := rs[0].Relations["descendants"]
	if len(pods) == 0 {
		t.Fatal("expected Pod descendants under ReplicaSet")
	}
	for _, pod := range pods {
		kind := getProjectedString(t, pod.Object, "kind")
		if kind != "Pod" {
			t.Fatalf("expected Pod at level 2, got %q", kind)
		}
	}
}
