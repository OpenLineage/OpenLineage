package facets

type FacetTypes interface {
	RunFacets | InputDatasetFacets | OutputDatasetFacets | DatasetFacets | JobFacets
}

type Facet[T FacetTypes] interface {
	// Apply sets this facet in the facets struct.
	// The argument is a pointer to a pointer.
	// If *T is a nil pointer, it is initialized with a zero value.
	Apply(facets **T)
}

type RunFacet Facet[RunFacets]
type DatasetFacet Facet[DatasetFacets]
type InputDatasetFacet Facet[InputDatasetFacets]
type OutputDatasetFacet Facet[OutputDatasetFacets]
type JobFacet Facet[JobFacets]
