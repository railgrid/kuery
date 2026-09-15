package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
	"github.com/railgrid/kuery/pkg/store"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func makeLinkedObject(cluster, namespace, name string, relatesToAnnotation []map[string]string) *store.ObjectModel {
	annotations := map[string]any{
		"kuery.io/relates-to": relatesToAnnotation,
	}
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":        name,
			"namespace":   namespace,
			"annotations": annotations,
		},
	}
	return &store.ObjectModel{
		ID:          uuid.New(),
		UID:         uuid.New().String(),
		Cluster:     cluster,
		APIGroup:    "",
		APIVersion:  "v1",
		Kind:        "ConfigMap",
		Resource:    "configmaps",
		Namespace:   namespace,
		Name:        name,
		Annotations: mustJSON(annotations),
		CreationTS:  ts("2025-06-01T00:00:00Z"),
		Object:      mustJSON(obj),
	}
}

func makeGroupedObject(cluster, namespace, name, group string) *store.ObjectModel {
	labels := map[string]string{"kuery.io/group": group}
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
	}
	return &store.ObjectModel{
		ID:         uuid.New(),
		UID:        uuid.New().String(),
		Cluster:    cluster,
		APIGroup:   "",
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Resource:   "configmaps",
		Namespace:  namespace,
		Name:       name,
		Labels:     datatypes.JSON(mustJSON(labels)),
		CreationTS: ts("2025-06-01T00:00:00Z"),
		Object:     mustJSON(obj),
	}
}

// --- Linked Relation Tests ---

func TestRelation_Linked_SameCluster(t *testing.T) {
	s := setupTestStore(t)
	// Source with relates-to annotation pointing to a Secret in the same cluster.
	source := makeLinkedObject("cluster-a", "default", "my-config",
		[]map[string]string{
			{"kind": "Secret", "namespace": "default", "name": "my-secret"},
		})
	target := makeSecret("cluster-a", "default", "my-secret")
	unrelated := makeSecret("cluster-a", "default", "other-secret")

	seedObjects(t, s, source, target, unrelated)

	e := NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "my-config"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Relations: map[string]v1alpha1.RelationSpec{
				"linked": {},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(status.Objects) != 1 {
		t.Fatalf("expected 1 root, got %d", len(status.Objects))
	}
	linked := status.Objects[0].Relations["linked"]
	if len(linked) != 1 {
		t.Fatalf("expected 1 linked object, got %d", len(linked))
	}
}

func TestRelation_Linked_CrossCluster(t *testing.T) {
	s := setupTestStore(t)
	// Source in cluster-a with annotation pointing to cluster-b.
	source := makeLinkedObject("cluster-a", "default", "my-config",
		[]map[string]string{
			{"cluster": "cluster-b", "kind": "Secret", "namespace": "default", "name": "shared-cert"},
		})
	target := makeSecret("cluster-b", "default", "shared-cert")
	wrongCluster := makeSecret("cluster-a", "default", "shared-cert")

	seedObjects(t, s, source, target, wrongCluster)

	e := NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "my-config"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Relations: map[string]v1alpha1.RelationSpec{
				"linked": {},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	linked := status.Objects[0].Relations["linked"]
	if len(linked) != 1 {
		t.Fatalf("expected 1 linked object (from cluster-b), got %d", len(linked))
	}
}

func TestRelation_Linked_MultipleTargets(t *testing.T) {
	s := setupTestStore(t)
	source := makeLinkedObject("cluster-a", "default", "my-config",
		[]map[string]string{
			{"kind": "Secret", "namespace": "default", "name": "secret-1"},
			{"cluster": "cluster-b", "kind": "Secret", "namespace": "default", "name": "secret-2"},
		})
	target1 := makeSecret("cluster-a", "default", "secret-1")
	target2 := makeSecret("cluster-b", "default", "secret-2")

	seedObjects(t, s, source, target1, target2)

	e := NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "my-config"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Relations: map[string]v1alpha1.RelationSpec{
				"linked": {},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	linked := status.Objects[0].Relations["linked"]
	if len(linked) != 2 {
		t.Fatalf("expected 2 linked objects, got %d", len(linked))
	}
}

// --- Grouped Relation Tests ---

func TestRelation_Grouped_SameCluster(t *testing.T) {
	s := setupTestStore(t)
	obj1 := makeGroupedObject("cluster-a", "default", "frontend", "my-app-stack")
	obj2 := makeGroupedObject("cluster-a", "default", "backend", "my-app-stack")
	obj3 := makeGroupedObject("cluster-a", "default", "database", "my-app-stack")
	unrelated := makeGroupedObject("cluster-a", "default", "other", "other-stack")

	seedObjects(t, s, obj1, obj2, obj3, unrelated)

	e := NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "frontend"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Relations: map[string]v1alpha1.RelationSpec{
				"grouped": {},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	grouped := status.Objects[0].Relations["grouped"]
	// frontend is queried, so grouped should return backend + database (same group, not self).
	if len(grouped) != 2 {
		t.Fatalf("expected 2 grouped objects, got %d", len(grouped))
	}
}

func TestRelation_Grouped_CrossCluster(t *testing.T) {
	s := setupTestStore(t)
	obj1 := makeGroupedObject("cluster-a", "default", "frontend", "my-app")
	obj2 := makeGroupedObject("cluster-b", "default", "backend", "my-app")

	seedObjects(t, s, obj1, obj2)

	e := NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "frontend"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Relations: map[string]v1alpha1.RelationSpec{
				"grouped": {},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	grouped := status.Objects[0].Relations["grouped"]
	if len(grouped) != 1 {
		t.Fatalf("expected 1 cross-cluster grouped object, got %d", len(grouped))
	}
}

// --- Annotation Parsing Tests ---

func TestLinkedAnnotation_Parsing(t *testing.T) {
	// Test that the annotation format is correctly stored and parsed.
	refs := []map[string]string{
		{"cluster": "c-b", "kind": "Secret", "namespace": "ns", "name": "s1"},
	}
	raw, err := json.Marshal(map[string]any{"kuery.io/relates-to": refs})
	if err != nil {
		t.Fatal(err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}

	relateTo := parsed["kuery.io/relates-to"].([]any)
	if len(relateTo) != 1 {
		t.Fatalf("expected 1 relates-to entry, got %d", len(relateTo))
	}

	entry := relateTo[0].(map[string]any)
	if entry["cluster"] != "c-b" || entry["kind"] != "Secret" {
		t.Fatalf("unexpected entry: %v", entry)
	}
}
