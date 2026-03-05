/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package facets provides OpenLineage facet types and constructors.
package facets

// FacetTypes is a type constraint for all facet container types.
type FacetTypes interface {
	RunFacets | InputDatasetFacets | OutputDatasetFacets | DatasetFacets | JobFacets
}

// Facet is implemented by all facet types that can be applied to a facet container.
type Facet[T FacetTypes] interface {
	// Apply sets this facet in the facets struct.
	// The argument is a pointer to a pointer.
	// If *T is a nil pointer, it is initialized with a zero value.
	Apply(facets **T)
}

// RunFacet is a facet that can be applied to a RunFacets container.
type RunFacet Facet[RunFacets]

// DatasetFacet is a facet that can be applied to a DatasetFacets container.
type DatasetFacet Facet[DatasetFacets]

// InputDatasetFacet is a facet that can be applied to an InputDatasetFacets container.
type InputDatasetFacet Facet[InputDatasetFacets]

// OutputDatasetFacet is a facet that can be applied to an OutputDatasetFacets container.
type OutputDatasetFacet Facet[OutputDatasetFacets]

// JobFacet is a facet that can be applied to a JobFacets container.
type JobFacet Facet[JobFacets]
