package engine

import (
	"fmt"
	"strings"

	"github.com/railgrid/kuery/apis/query/v1alpha1"
)

// RelationType identifies the supported relation types (non-transitive).
// Transitive variants (+ suffix) are handled in Phase 5.
const (
	RelOwners      = "owners"
	RelDescendants = "descendants"
	RelReferences  = "references"
	RelSelects     = "selects"
	RelSelectedBy  = "selected-by"
	RelEvents      = "events"
	RelLinked      = "linked"
	RelGrouped     = "grouped"
	// RelNamespace: a namespaced object → the Namespace object it lives in.
	// RelNamespaced is the reverse: a Namespace → every object it contains.
	// Both are intra-cluster (a Namespace and its members share a cluster).
	RelNamespace  = "namespace"
	RelNamespaced = "namespaced"
	// RelMembers: a cluster node → every object in that cluster. Used with a
	// clusters-rooted query (spec.root=clusters) to expand the per-cluster
	// tree. The parent's cluster column carries the cluster name.
	RelMembers = "members"
)

// RelationDirection describes how deletion impact flows along a relation, from
// the queried (anchor) object's point of view. It is the canonical source of
// truth for any consumer that renders or reasons about impact (the portal graph
// arrows, the kuery_impact MCP tool) so they don't each re-derive it.
type RelationDirection string

const (
	// DirectionUpstream: the related object is a dependency — deleting it
	// impacts the anchor. The anchor depends on it. (owners, references,
	// selects, namespace)
	DirectionUpstream RelationDirection = "upstream"
	// DirectionDownstream: the related object is in the anchor's blast radius —
	// deleting the anchor impacts it. (descendants, selected-by, namespaced,
	// members, events)
	DirectionDownstream RelationDirection = "downstream"
	// DirectionLateral: peer association with no clear deletion direction.
	// (linked, grouped)
	DirectionLateral RelationDirection = "lateral"
)

// RelationDirections maps each relation (base name, no "+") to its impact
// direction. Reverse pairs sit on opposite sides — e.g. owners is the upstream
// reverse of descendants — which is exactly what keeps a Namespace as a pod's
// parent (it impacts the pod) rather than a child.
var RelationDirections = map[string]RelationDirection{
	RelOwners:      DirectionUpstream,
	RelReferences:  DirectionUpstream,
	RelSelects:     DirectionUpstream,
	RelNamespace:   DirectionUpstream,
	RelDescendants: DirectionDownstream,
	RelSelectedBy:  DirectionDownstream,
	RelNamespaced:  DirectionDownstream,
	RelMembers:     DirectionDownstream,
	RelEvents:      DirectionDownstream,
	RelLinked:      DirectionLateral,
	RelGrouped:     DirectionLateral,
}

// RelationReverse maps a relation to the one that traverses the same edge in
// the opposite direction (owners↔descendants, selects↔selected-by,
// namespace↔namespaced). Relations without a reverse (linked, grouped, members,
// events) are absent.
var RelationReverse = map[string]string{
	RelOwners:      RelDescendants,
	RelDescendants: RelOwners,
	RelSelects:     RelSelectedBy,
	RelSelectedBy:  RelSelects,
	RelNamespace:   RelNamespaced,
	RelNamespaced:  RelNamespace,
}

// DirectionOf returns the impact direction of a relation name (accepts the
// transitive "+" form). Unknown relations default to downstream — the safe
// "this is in my blast radius" assumption.
func DirectionOf(name string) RelationDirection {
	if d, ok := RelationDirections[BaseRelation(name)]; ok {
		return d
	}
	return DirectionDownstream
}

// IsTransitive returns true if the relation name ends with "+".
func IsTransitive(name string) bool {
	return strings.HasSuffix(name, "+")
}

// BaseRelation strips the "+" suffix from a relation name.
func BaseRelation(name string) string {
	return strings.TrimSuffix(name, "+")
}

// relationContext carries information needed to generate a relation JOIN.
type relationContext struct {
	parentAlias string // alias of the parent object (e.g., "l0")
	childAlias  string // alias of the child/target object (e.g., "l1")
	dialect     string
	filters     []v1alpha1.ObjectFilter // relation-level filters
	refRegistry *RefPathRegistry
}

// buildRelationJoin generates the JOIN clause and additional WHERE conditions
// for a given relation type. Returns (joinClause, extraWhere, args).
func buildRelationJoin(relationType string, ctx relationContext) (string, []string, []any) {
	switch relationType {
	case RelDescendants:
		return buildDescendantJoin(ctx)
	case RelOwners:
		return buildOwnerJoin(ctx)
	case RelReferences:
		return buildReferenceJoin(ctx)
	case RelSelects:
		return buildSelectsJoin(ctx)
	case RelSelectedBy:
		return buildSelectedByJoin(ctx)
	case RelEvents:
		return buildEventsJoin(ctx)
	case RelLinked:
		return buildLinkedJoin(ctx)
	case RelGrouped:
		return buildGroupedJoin(ctx)
	case RelNamespace:
		return buildNamespaceJoin(ctx)
	case RelNamespaced:
		return buildNamespacedJoin(ctx)
	case RelMembers:
		return buildMembersJoin(ctx)
	default:
		// Unknown relation type — return empty (will produce no results).
		return fmt.Sprintf("JOIN objects %s ON 1=0", ctx.childAlias), nil, nil
	}
}

// buildDescendantJoin: parent → children via ownerRefs.
// Children whose ownerRefs contain the parent's UID.
func buildDescendantJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.owner_refs @> jsonb_build_array(jsonb_build_object('uid', %s.uid))",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias)
	default: // sqlite
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND EXISTS (SELECT 1 FROM json_each(%s.owner_refs) oref WHERE json_extract(oref.value, '$.uid') = %s.uid)",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias)
	}
	return join, nil, nil
}

// buildOwnerJoin: child → parent via ownerRefs.
// Parents whose UID appears in the child's ownerRefs.
func buildOwnerJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		// Guard jsonb_array_elements against rows whose owner_refs is a JSON
		// scalar (e.g. legacy "null" rows) — it errors with "cannot extract
		// elements from a scalar" otherwise.
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.uid IN (SELECT ref->>'uid' FROM jsonb_array_elements("+
				"CASE WHEN jsonb_typeof(%s.owner_refs) = 'array' THEN %s.owner_refs ELSE '[]'::jsonb END) ref)",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias, ctx.parentAlias)
	default: // sqlite
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.uid IN (SELECT json_extract(oref.value, '$.uid') FROM json_each(%s.owner_refs) oref)",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias)
	}
	return join, nil, nil
}

// buildReferenceJoin: source → target via spec field references (ref-path registry).
// Uses the ref-path registry to find which fields in the source reference which target kinds.
func buildReferenceJoin(ctx relationContext) (string, []string, []any) {
	if ctx.refRegistry == nil {
		return fmt.Sprintf("JOIN objects %s ON 1=0", ctx.childAlias), nil, nil
	}

	// Determine target kind filter from relation filters.
	var targetKinds []string
	for _, f := range ctx.filters {
		if f.GroupKind != nil && f.GroupKind.Kind != "" {
			targetKinds = append(targetKinds, f.GroupKind.Kind)
		}
	}

	// Build a UNION of all ref-path subqueries that match the target kinds.
	// If no target kind filter, use all ref paths from all source kinds.
	// Since we don't know the source kind at SQL-generation time (it's dynamic),
	// we generate a broad match using CASE WHEN on the parent's kind.
	//
	// For simplicity, we generate the join condition as:
	// target.name IN (ref_subquery) AND target.cluster = parent.cluster AND target.namespace = parent.namespace
	// with a CASE on parent.kind to select the right ref paths.

	// Collect ref paths and build name lookup subqueries.
	allPaths := BuiltinRefPaths
	var nameSubqueries []string

	for _, rp := range allPaths {
		if len(targetKinds) > 0 {
			found := false
			for _, tk := range targetKinds {
				if strings.EqualFold(rp.TargetKind, tk) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Each ref subquery returns a set of names.
		// Strip the outer parentheses from BuildRefNameSubquery since we'll use it in UNION ALL.
		subq := rp.BuildRefNameSubquery(ctx.parentAlias, ctx.dialect)
		// Remove surrounding parens: "(SELECT ...)" → "SELECT ..."
		subq = strings.TrimPrefix(subq, "(")
		subq = strings.TrimSuffix(subq, ")")
		nameSubqueries = append(nameSubqueries, subq)
	}

	if len(nameSubqueries) == 0 {
		return fmt.Sprintf("JOIN objects %s ON 1=0", ctx.childAlias), nil, nil
	}

	combinedSubquery := strings.Join(nameSubqueries, " UNION ALL ")

	join := fmt.Sprintf(
		"JOIN objects %s ON %s.cluster = %s.cluster "+
			"AND %s.namespace = %s.namespace "+
			"AND %s.name IN (%s)",
		ctx.childAlias, ctx.childAlias, ctx.parentAlias,
		ctx.childAlias, ctx.parentAlias,
		ctx.childAlias, combinedSubquery)

	// Add target kind filter.
	var extraWhere []string
	if len(targetKinds) > 0 {
		placeholders := make([]string, len(targetKinds))
		var args []any
		for i, tk := range targetKinds {
			placeholders[i] = "?"
			args = append(args, tk)
		}
		extraWhere = append(extraWhere,
			fmt.Sprintf("%s.kind IN (%s)", ctx.childAlias, strings.Join(placeholders, ",")))
		return join, extraWhere, args
	}

	return join, nil, nil
}

// buildSelectsJoin: selector holder → matched objects.
// The parent holds a selector (spec.selector.matchLabels), find targets whose labels match.
func buildSelectsJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.namespace = %s.namespace "+
				"AND %s.labels @> (%s.object->'spec'->'selector'->'matchLabels') "+
				"AND %s.object->'spec'->'selector'->'matchLabels' IS NOT NULL "+
				"AND %s.id != %s.id",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias)
	default: // sqlite
		// For SQLite, check every key-value in parent's matchLabels exists in target's labels.
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.namespace = %s.namespace "+
				"AND json_extract(%s.object, '$.spec.selector.matchLabels') IS NOT NULL "+
				"AND %s.id != %s.id "+
				"AND NOT EXISTS ("+
				"SELECT 1 FROM json_each(json_extract(%s.object, '$.spec.selector.matchLabels')) sel "+
				"WHERE json_extract(%s.labels, '$.' || sel.key) IS NULL "+
				"OR json_extract(%s.labels, '$.' || sel.key) != sel.value"+
				")",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias)
	}
	return join, nil, nil
}

// buildSelectedByJoin: matched → selector holder (reverse of selects).
// The child holds a selector, parent's labels match it.
func buildSelectedByJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.namespace = %s.namespace "+
				"AND %s.labels @> (%s.object->'spec'->'selector'->'matchLabels') "+
				"AND %s.object->'spec'->'selector'->'matchLabels' IS NOT NULL "+
				"AND %s.id != %s.id",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias, ctx.childAlias,
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias)
	default: // sqlite
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.namespace = %s.namespace "+
				"AND json_extract(%s.object, '$.spec.selector.matchLabels') IS NOT NULL "+
				"AND %s.id != %s.id "+
				"AND NOT EXISTS ("+
				"SELECT 1 FROM json_each(json_extract(%s.object, '$.spec.selector.matchLabels')) sel "+
				"WHERE json_extract(%s.labels, '$.' || sel.key) IS NULL "+
				"OR json_extract(%s.labels, '$.' || sel.key) != sel.value"+
				")",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.parentAlias,
			ctx.parentAlias)
	}
	return join, nil, nil
}

// buildEventsJoin: object → events matching involvedObject.uid.
func buildEventsJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.kind = 'Event' "+
				"AND %s.object->'involvedObject'->>'uid' = %s.uid",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias)
	default: // sqlite
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.cluster = %s.cluster "+
				"AND %s.kind = 'Event' "+
				"AND json_extract(%s.object, '$.involvedObject.uid') = %s.uid",
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias)
	}
	return join, nil, nil
}

// buildLinkedJoin: source → target via kuery.io/relates-to annotation.
// Cross-cluster: target can be in any cluster.
func buildLinkedJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		// Wrap the annotation payload in a typeof guard: a malformed or
		// non-array relates-to value would otherwise abort the whole query
		// with "cannot extract elements from a scalar".
		join = fmt.Sprintf(
			"JOIN LATERAL jsonb_array_elements("+
				"CASE WHEN jsonb_typeof((%s.object->'metadata'->'annotations'->>'kuery.io/relates-to')::jsonb) = 'array' "+
				"THEN (%s.object->'metadata'->'annotations'->>'kuery.io/relates-to')::jsonb ELSE '[]'::jsonb END"+
				") AS ref ON true "+
				"JOIN objects %s ON %s.cluster = COALESCE(ref->>'cluster', %s.cluster) "+
				"AND %s.api_group = COALESCE(ref->>'group', '') "+
				"AND %s.kind = ref->>'kind' "+
				"AND %s.namespace = COALESCE(ref->>'namespace', '') "+
				"AND %s.name = ref->>'name'",
			ctx.parentAlias, ctx.parentAlias,
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias,
			ctx.childAlias,
			ctx.childAlias)
	default: // sqlite
		// The annotation value is a JSON array string stored in the annotations JSONB.
		join = fmt.Sprintf(
			"JOIN json_each(json_extract(%s.annotations, '$.\"kuery.io/relates-to\"')) ref ON 1=1 "+
				"JOIN objects %s ON %s.cluster = COALESCE(json_extract(ref.value, '$.cluster'), %s.cluster) "+
				"AND %s.api_group = COALESCE(json_extract(ref.value, '$.group'), '') "+
				"AND %s.kind = json_extract(ref.value, '$.kind') "+
				"AND %s.namespace = COALESCE(json_extract(ref.value, '$.namespace'), '') "+
				"AND %s.name = json_extract(ref.value, '$.name')",
			ctx.parentAlias,
			ctx.childAlias, ctx.childAlias, ctx.parentAlias,
			ctx.childAlias,
			ctx.childAlias,
			ctx.childAlias,
			ctx.childAlias)
	}
	return join, nil, nil
}

// buildGroupedJoin: bidirectional grouping via kuery.io/group label.
// Cross-cluster: matches objects in any cluster with the same group label.
func buildGroupedJoin(ctx relationContext) (string, []string, []any) {
	var join string
	switch ctx.dialect {
	case "postgres":
		join = fmt.Sprintf(
			"JOIN objects %s ON %s.labels->>'kuery.io/group' = %s.labels->>'kuery.io/group' "+
				"AND %s.id != %s.id "+
				"AND %s.labels->>'kuery.io/group' IS NOT NULL",
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias)
	default: // sqlite
		join = fmt.Sprintf(
			"JOIN objects %s ON json_extract(%s.labels, '$.\"kuery.io/group\"') = json_extract(%s.labels, '$.\"kuery.io/group\"') "+
				"AND %s.id != %s.id "+
				"AND json_extract(%s.labels, '$.\"kuery.io/group\"') IS NOT NULL",
			ctx.childAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.childAlias, ctx.parentAlias,
			ctx.parentAlias)
	}
	return join, nil, nil
}

// buildMembersJoin: cluster node → every object in that cluster. The parent
// (a clusters-rooted node) carries the cluster name in its cluster column, so
// the join is plain column equality. RelationSpec.Filters/Limit (applied by
// the generator) narrow the set, e.g. to one kind tier. Dialect-agnostic.
func buildMembersJoin(ctx relationContext) (string, []string, []any) {
	join := fmt.Sprintf(
		"JOIN objects %s ON %s.cluster = %s.cluster",
		ctx.childAlias, ctx.childAlias, ctx.parentAlias)
	return join, nil, nil
}

// buildNamespaceJoin: namespaced object → the Namespace object it lives in.
// A Namespace is the core-group object kind=Namespace whose name equals the
// parent's namespace, in the same cluster. Cluster-scoped parents (namespace
// ”) resolve to nothing. Dialect-agnostic: pure column equality, no JSON.
func buildNamespaceJoin(ctx relationContext) (string, []string, []any) {
	join := fmt.Sprintf(
		"JOIN objects %s ON %s.cluster = %s.cluster "+
			"AND %s.kind = 'Namespace' AND %s.api_group = '' "+
			"AND %s.name = %s.namespace "+
			"AND %s.namespace != ''",
		ctx.childAlias, ctx.childAlias, ctx.parentAlias,
		ctx.childAlias, ctx.childAlias,
		ctx.childAlias, ctx.parentAlias,
		ctx.parentAlias)
	return join, nil, nil
}

// buildNamespacedJoin: Namespace object → every object it contains (reverse of
// buildNamespaceJoin). Members share the cluster and carry namespace = the
// Namespace's name. Only fires when the parent is itself a Namespace. The
// id guard keeps the Namespace from listing itself (its own namespace is ”,
// so it can't match anyway, but it's cheap insurance). Dialect-agnostic.
func buildNamespacedJoin(ctx relationContext) (string, []string, []any) {
	join := fmt.Sprintf(
		"JOIN objects %s ON %s.cluster = %s.cluster "+
			"AND %s.kind = 'Namespace' AND %s.api_group = '' "+
			"AND %s.namespace = %s.name "+
			"AND %s.id != %s.id",
		ctx.childAlias, ctx.childAlias, ctx.parentAlias,
		ctx.parentAlias, ctx.parentAlias,
		ctx.childAlias, ctx.parentAlias,
		ctx.childAlias, ctx.parentAlias)
	return join, nil, nil
}
