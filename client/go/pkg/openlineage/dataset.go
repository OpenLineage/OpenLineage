package openlineage

import (
	"time"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

type DatasetEvent struct {
	Dataset Dataset

	BaseEvent
}

func (e *DatasetEvent) AsEmittable() Event {
	return Event{
		EventTime: e.EventTime,
		Dataset:   &e.Dataset,
		Producer:  e.Producer,
		SchemaURL: e.SchemaURL,
	}
}

func NewDatasetEvent(
	name string,
	namespace string,
	producer string,
	facets ...facets.DatasetFacet,
) DatasetEvent {
	return DatasetEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: DatasetEventSchemaURL,
			EventTime: time.Now(),
		},
		Dataset: NewDataset(name, namespace, facets...),
	}
}

func NewDataset(name string, namespace string, datasetFacets ...facets.DatasetFacet) Dataset {
	var dataset *facets.DatasetFacets
	for _, f := range datasetFacets {
		f.Apply(&dataset)
	}

	return Dataset{
		Name:      name,
		Namespace: namespace,
		Facets:    dataset,
	}
}
