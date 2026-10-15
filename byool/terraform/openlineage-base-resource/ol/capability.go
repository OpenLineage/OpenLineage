/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

// JobFacet identifies a facet that applies to the job itself (not its inputs/outputs).
// Only accepted by JobCapability.WithFacetEnabled — passing a JobFacet to
// DatasetCapability.WithFacetEnabled is a compile-time error.
type JobFacet int

// DatasetFacet identifies a facet that applies to a dataset.
// Accepted by DatasetCapability.WithFacetEnabled (standalone dataset resources) and by
// JobCapability.WithDatasetFacetEnabled (to control facets emitted on a job's inputs/outputs).
type DatasetFacet int

const (
	// ── Job facets ────────────────────────────────────────────────────────────
	// These are only meaningful for job resources.
	FacetJobType JobFacet = iota
	FacetJobOwnership
	FacetJobDocumentation
	FacetJobSourceCode
	FacetJobSourceCodeLocation
	FacetJobSQL
	FacetJobTags
)

const (
	// ── Dataset facets ────────────────────────────────────────────────────────
	// Meaningful with DatasetCapability and with JobCapability.WithDatasetFacetEnabled
	// (the latter controls facets emitted on a job's inputs and outputs).
	FacetDatasetSymlinks DatasetFacet = iota
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

// capability is the shared enabled-facet store.
// Two separate maps prevent iota collisions between JobFacet and DatasetFacet.
// Not exported directly — consumers use JobCapability or DatasetCapability.
type capability struct {
	jobEnabled     map[JobFacet]bool
	datasetEnabled map[DatasetFacet]bool
}

func (c capability) isJobEnabled(f JobFacet) bool         { return c.jobEnabled[f] }
func (c capability) isDatasetEnabled(f DatasetFacet) bool { return c.datasetEnabled[f] }

func (c capability) withJobFacets(fs ...JobFacet) capability {
	jobNext := make(map[JobFacet]bool, len(c.jobEnabled)+len(fs))
	for f := range c.jobEnabled {
		jobNext[f] = true
	}
	for _, f := range fs {
		jobNext[f] = true
	}
	dsNext := make(map[DatasetFacet]bool, len(c.datasetEnabled))
	for f := range c.datasetEnabled {
		dsNext[f] = true
	}
	return capability{jobEnabled: jobNext, datasetEnabled: dsNext}
}

func (c capability) withDatasetFacets(fs ...DatasetFacet) capability {
	jobNext := make(map[JobFacet]bool, len(c.jobEnabled))
	for f := range c.jobEnabled {
		jobNext[f] = true
	}
	dsNext := make(map[DatasetFacet]bool, len(c.datasetEnabled)+len(fs))
	for f := range c.datasetEnabled {
		dsNext[f] = true
	}
	for _, f := range fs {
		dsNext[f] = true
	}
	return capability{jobEnabled: jobNext, datasetEnabled: dsNext}
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
	return JobCapability{capability{
		jobEnabled:     map[JobFacet]bool{},
		datasetEnabled: map[DatasetFacet]bool{},
	}}
}

// WithFacetEnabled returns a new JobCapability with the given job facets enabled.
// To control which dataset facets are emitted on inputs/outputs, use WithDatasetFacetEnabled.
func (c JobCapability) WithFacetEnabled(facets ...JobFacet) JobCapability {
	return JobCapability{c.withJobFacets(facets...)}
}

// WithDatasetFacetEnabled returns a new JobCapability with the given dataset facets
// enabled for this job's inputs and outputs.
func (c JobCapability) WithDatasetFacetEnabled(facets ...DatasetFacet) JobCapability {
	return JobCapability{c.withDatasetFacets(facets...)}
}

// IsEnabled reports whether a job facet is active in this capability.
func (c JobCapability) IsEnabled(f JobFacet) bool { return c.isJobEnabled(f) }

// IsDatasetEnabled reports whether a dataset facet is active in this capability.
func (c JobCapability) IsDatasetEnabled(f DatasetFacet) bool { return c.isDatasetEnabled(f) }

// ── DatasetCapability ─────────────────────────────────────────────────────────

// DatasetCapability declares which dataset facets a consumer supports for
// standalone dataset resources.
// Only DatasetFacet constants are accepted — passing a JobFacet is a compile-time error.
type DatasetCapability struct{ capability }

// EmptyDatasetCapability returns a DatasetCapability with no facets enabled.
// New facets introduced to the spec will be stubs by default, requiring
// providers to opt in explicitly — preventing accidental support.
func EmptyDatasetCapability() DatasetCapability {
	return DatasetCapability{capability{
		jobEnabled:     map[JobFacet]bool{},
		datasetEnabled: map[DatasetFacet]bool{},
	}}
}

// WithFacetEnabled returns a new DatasetCapability with the given dataset facets enabled.
// Job facets (FacetJob*) are intentionally not accepted here — they have no meaning for
// standalone dataset resources. Enable them via JobCapability.WithFacetEnabled instead.
func (c DatasetCapability) WithFacetEnabled(facets ...DatasetFacet) DatasetCapability {
	return DatasetCapability{c.withDatasetFacets(facets...)}
}

// IsEnabled reports whether a dataset facet is active in this capability.
func (c DatasetCapability) IsEnabled(f DatasetFacet) bool { return c.isDatasetEnabled(f) }

// IsDatasetEnabled is an alias for IsEnabled, provided for consistency with
// JobCapability so both types can be used interchangeably in shared helpers.
func (c DatasetCapability) IsDatasetEnabled(f DatasetFacet) bool { return c.isDatasetEnabled(f) }
