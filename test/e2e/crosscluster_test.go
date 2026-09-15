//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestCrossCluster_Linked(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Name: "app-config", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Cluster: true,
			Object:  proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"linked": {Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected app-config ConfigMap")
	}

	root := status.Objects[0]
	if root.Cluster != "cluster-a" {
		t.Fatalf("expected root in cluster-a, got %q", root.Cluster)
	}

	linked := root.Relations["linked"]
	if len(linked) == 0 {
		t.Fatal("expected linked relation to find shared-cert in cluster-b")
	}

	// Verify the linked object is the Secret from cluster-b.
	linkedName := getProjectedString(t, linked[0].Object, "metadata", "name")
	if linkedName != "shared-cert" {
		t.Fatalf("expected linked to shared-cert, got %q", linkedName)
	}
	if linked[0].Cluster != "cluster-b" {
		t.Fatalf("expected linked object in cluster-b, got %q", linked[0].Cluster)
	}
}

func TestCrossCluster_Grouped(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	// Query the nginx Deployment (has kuery.io/group: my-app).
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Cluster: true,
			Object:  proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"grouped": {Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}

	grouped := status.Objects[0].Relations["grouped"]
	if len(grouped) == 0 {
		t.Fatal("expected grouped results from my-app group")
	}

	// Should contain objects from both clusters.
	var clusters []string
	for _, g := range grouped {
		clusters = append(clusters, g.Cluster)
	}

	hasClusterB := false
	for _, c := range clusters {
		if c == "cluster-b" {
			hasClusterB = true
		}
	}
	if !hasClusterB {
		t.Fatalf("expected grouped to include objects from cluster-b, got clusters: %v", clusters)
	}

	// Verify names include cross-cluster objects.
	names := objectNames(t, grouped)
	// cluster-b objects with kuery.io/group: my-app: shared-cert, redis, redis-config
	hasCrossCluster := containsString(names, "shared-cert") || containsString(names, "redis") || containsString(names, "redis-config")
	if !hasCrossCluster {
		t.Fatalf("expected cross-cluster objects in grouped, got %v", names)
	}
}

func TestCrossCluster_GroupedExcludesSelf(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Cluster: true,
			Object:  proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"grouped": {Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}
	grouped := status.Objects[0].Relations["grouped"]
	rootName := getProjectedString(t, status.Objects[0].Object, "metadata", "name")

	// The root object itself should NOT appear in grouped results.
	for _, g := range grouped {
		name := getProjectedString(t, g.Object, "metadata", "name")
		cluster := g.Cluster
		if name == rootName && cluster == "cluster-a" {
			t.Fatal("grouped should not include self")
		}
	}
}
