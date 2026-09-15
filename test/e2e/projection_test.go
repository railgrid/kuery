//go:build e2e

package e2e_test

import (
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

func TestProjection_SparseFields(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: projectionSpec(map[string]any{
				"metadata": map[string]any{"name": true, "namespace": true},
				"spec":     map[string]any{"replicas": true},
			}),
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected at least 1 result")
	}
	obj := status.Objects[0]
	name := getProjectedString(t, obj.Object, "metadata", "name")
	if name != "nginx" {
		t.Fatalf("expected name=nginx, got %q", name)
	}
	replicas := getProjectedValue(t, obj.Object, "spec", "replicas")
	if replicas == nil {
		t.Fatal("expected spec.replicas in projection")
	}
	// Verify labels is NOT present (not requested).
	labels := getProjectedValue(t, obj.Object, "metadata", "labels")
	if labels != nil {
		t.Fatal("expected metadata.labels to be absent from projection")
	}
}

func TestProjection_MetadataFields(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Name: "nginx", Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			ID:          true,
			Cluster:     true,
			MutablePath: true,
		},
	})
	if len(status.Objects) == 0 {
		t.Fatal("expected at least 1 result")
	}
	obj := status.Objects[0]
	if obj.ID == "" {
		t.Fatal("expected ID to be populated")
	}
	if obj.Cluster == "" {
		t.Fatal("expected Cluster to be populated")
	}
	if obj.MutablePath == "" {
		t.Fatal("expected MutablePath to be populated")
	}
	if obj.MutablePath != "/apis/apps/v1/namespaces/demo/deployments/nginx" {
		t.Fatalf("unexpected mutablePath: %s", obj.MutablePath)
	}
}

func TestProjection_KindInObject(t *testing.T) {
	t.Parallel()
	status := queryKuery(t, v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}, Namespace: "demo"},
			},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: projectionSpec(map[string]any{"kind": true, "metadata": map[string]any{"name": true}}),
		},
	})
	for _, obj := range status.Objects {
		kind := getProjectedString(t, obj.Object, "kind")
		if kind != "Deployment" {
			t.Fatalf("expected kind=Deployment, got %q", kind)
		}
	}
}
