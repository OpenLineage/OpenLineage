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

// capability is the shared disabled-facet set.
// Not exported directly — consumers use JobCapability or DatasetCapability.
type capability struct {
	disabled map[Facet]bool
}

func (c capability) IsDisabled(f Facet) bool { return c.disabled[f] }

func (c capability) withFacetDisabled(facets ...Facet) capability {
	next := make(map[Facet]bool, len(c.disabled)+len(facets))
	for f := range c.disabled {
		next[f] = true
	}
	for _, f := range facets {
		next[f] = true
	}
	return capability{disabled: next}
}

// ── JobCapability ─────────────────────────────────────────────────────────────

// JobCapability declares which facets a consumer supports for job resources.
// SchemaGenerator.JobSchema uses it to include only the relevant blocks.
type JobCapability struct{ capability }

// FullJobCapability returns a JobCapability with every facet enabled.
func FullJobCapability() JobCapability {
	return JobCapability{capability{disabled: map[Facet]bool{}}}
}

// WithFacetDisabled returns a new JobCapability with the given facets disabled.
func (c JobCapability) WithFacetDisabled(facets ...Facet) JobCapability {
	return JobCapability{c.capability.withFacetDisabled(facets...)}
}

// ── DatasetCapability ─────────────────────────────────────────────────────────

// DatasetCapability declares which dataset facets a consumer supports for
// standalone dataset resources. Column lineage is disabled by default —
// it is a job-output concept, not meaningful on a DatasetEvent.
type DatasetCapability struct{ capability }

// FullDatasetCapability returns a DatasetCapability with every dataset facet enabled
// except column lineage.
func FullDatasetCapability() DatasetCapability {
	return DatasetCapability{capability{disabled: map[Facet]bool{
		FacetDatasetColumnLineage: true,
	}}}
}

// WithFacetDisabled returns a new DatasetCapability with the given facets disabled.
func (c DatasetCapability) WithFacetDisabled(facets ...Facet) DatasetCapability {
	return DatasetCapability{c.capability.withFacetDisabled(facets...)}
}
