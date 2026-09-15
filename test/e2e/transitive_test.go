//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestTransitive_DescendantsPlus(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Cluster: true,
			Object:  proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants+": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}
	root := status.Objects[0]
	descendants := root.Relations["descendants+"]
	if len(descendants) == 0 {
		t.Fatal("expected transitive descendants")
	}
	// First level: ReplicaSet.
	rsKind := getProjectedString(t, descendants[0].Object, "kind")
	if rsKind != "ReplicaSet" {
		t.Fatalf("expected ReplicaSet, got %q", rsKind)
	}
	// Second level: Pods nested under RS.
	pods := descendants[0].Relations["descendants+"]
	if len(pods) == 0 {
		t.Fatal("expected Pods nested under ReplicaSet")
	}
	for _, pod := range pods {
		kind := getProjectedString(t, pod.Object, "kind")
		if kind != "Pod" {
			t.Fatalf("expected Pod, got %q", kind)
		}
	}
}

func TestTransitive_OwnersPlus(t *testing.T) {
	t.Parallel()
	proj := projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}})
	// Find a Pod name first.
	podStatus := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{Kind: "Pod"}, Namespace: "demo", Labels: map[string]string{"app": "nginx"}},
			},
		},
		Limit:   1,
		Objects: &v1alpha1.ObjectsSpec{Object: proj},
	})
	if len(podStatus.Objects) == 0 {
		t.Skip("no nginx Pods found")
	}
	podName := getProjectedString(t, podStatus.Objects[0].Object, "metadata", "name")

	// Query Pod with owners+.
	status := queryKuery(t, v1alpha1.QuerySpec{
		Cluster: &v1alpha1.ClusterFilter{Name: "cluster-a"},
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: podName, Namespace: "demo"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"owners+": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected Pod")
	}
	owners := status.Objects[0].Relations["owners+"]
	if len(owners) == 0 {
		t.Fatal("expected transitive owners (RS)")
	}
	// RS should have nested owner (Deployment).
	rsOwners := owners[0].Relations["owners+"]
	if len(rsOwners) == 0 {
		t.Fatal("expected Deployment as transitive owner of RS")
	}
	deployKind := getProjectedString(t, rsOwners[0].Object, "kind")
	if deployKind != "Deployment" {
		t.Fatalf("expected Deployment, got %q", deployKind)
	}
}

func TestTransitive_DescendantsPlusWithRefs(t *testing.T) {
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
			Object: proj,
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants+": {
					Objects: &v1alpha1.ObjectsSpec{
						Object: proj,
						Relations: map[string]v1alpha1.RelationSpec{
							"references": {Objects: &v1alpha1.ObjectsSpec{Object: proj}},
						},
					},
				},
			},
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected nginx deployment")
	}

	// Walk to Pods and check references.
	rs := status.Objects[0].Relations["descendants+"]
	if len(rs) == 0 {
		t.Fatal("expected RS")
	}
	pods := rs[0].Relations["descendants+"]
	if len(pods) == 0 {
		t.Fatal("expected Pods")
	}
	// Pods should have references to Secrets, ConfigMaps, PVCs.
	refs := pods[0].Relations["references"]
	if len(refs) == 0 {
		t.Fatal("expected references on Pod")
	}
	refNames := objectNames(t, refs)
	if !containsString(refNames, "nginx-tls") {
		t.Fatalf("expected nginx-tls Secret in references, got %v", refNames)
	}
	if !containsString(refNames, "nginx-config") {
		t.Fatalf("expected nginx-config ConfigMap in references, got %v", refNames)
	}
	if !containsString(refNames, "nginx-data") {
		t.Fatalf("expected nginx-data PVC in references, got %v", refNames)
	}
}
