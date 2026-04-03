/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

// Facet identifies a single optional facet in the OL schema.
// Used by JobCapability / DatasetCapability to declare what a consumer supports.
type Facet int

const (
	// ── Job facets ────────────────────────────────────────────────────────────
	FacetJobType Facet = iota
	FacetJobOwnership
	FacetJobDocumentation
	FacetJobSourceCode
	FacetJobSourceCodeLocation
	FacetJobSQL
	FacetJobTags

	// ── Dataset facets (shared by inputs, outputs and standalone datasets) ────
	FacetDatasetSymlinks
	FacetDatasetSchema
	FacetDatasetDataSource
	FacetDatasetDocumentation
	FacetDatasetType
	FacetDatasetVersion
	FacetDatasetStorage
	FacetDatasetOwnership
	FacetDatasetLifecycleStateChange
	FacetDatasetHierarchy
	FacetDatasetCatalog
	FacetDatasetColumnLineage // only meaningful on job outputs
	FacetDatasetTags
)

// capability is the shared enabled-facet set.
// Additive model: only explicitly enabled facets are active.
// New facets added to the spec are stubs by default until a consumer opts in.
// Not exported directly — consumers use JobCapability or DatasetCapability.
type capability struct {
	enabled map[Facet]bool
}

func (c capability) IsEnabled(f Facet) bool { return c.enabled[f] }

func (c capability) withFacetEnabled(facets ...Facet) capability {
	next := make(map[Facet]bool, len(c.enabled)+len(facets))
	for f := range c.enabled {
		next[f] = true
	}
	for _, f := range facets {
		next[f] = true
	}
	return capability{enabled: next}
}

// ── JobCapability ─────────────────────────────────────────────────────────────

// JobCapability declares which facets a consumer supports for job resources.
// Start from EmptyJobCapability and enable the facets the target system handles.
// Any facet not explicitly enabled is included as an Optional+Computed stub so
// config copied from another consumer is accepted without error.
type JobCapability struct{ capability }

// EmptyJobCapability returns a JobCapability with no facets enabled.
// New facets introduced to the spec will be stubs by default, requiring
// providers to opt in explicitly — preventing accidental support.
func EmptyJobCapability() JobCapability {
	return JobCapability{capability{enabled: map[Facet]bool{}}}
}

// WithFacetEnabled returns a new JobCapability with the given facets enabled.
func (c JobCapability) WithFacetEnabled(facets ...Facet) JobCapability {
	return JobCapability{c.capability.withFacetEnabled(facets...)}
}

// ── DatasetCapability ─────────────────────────────────────────────────────────

// DatasetCapability declares which dataset facets a consumer supports for
// standalone dataset resources.
type DatasetCapability struct{ capability }

// EmptyDatasetCapability returns a DatasetCapability with no facets enabled.
// New facets introduced to the spec will be stubs by default, requiring
// providers to opt in explicitly — preventing accidental support.
func EmptyDatasetCapability() DatasetCapability {
	return DatasetCapability{capability{enabled: map[Facet]bool{}}}
}

// WithFacetEnabled returns a new DatasetCapability with the given facets enabled.
func (c DatasetCapability) WithFacetEnabled(facets ...Facet) DatasetCapability {
	return DatasetCapability{c.capability.withFacetEnabled(facets...)}
}
