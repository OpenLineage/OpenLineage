/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"time"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
)

// NewDatasetEvent creates a new DatasetEvent with the given dataset and producer.
func NewDatasetEvent(
	name string,
	namespace string,
	producer string,
	fs ...facets.DatasetFacet,
) *DatasetEvent {
	return &DatasetEvent{
		BaseEvent: BaseEvent{
			Producer:  producer,
			SchemaURL: DatasetEventSchemaURL,
			EventTime: time.Now(),
		},
		Dataset: NewDataset(name, namespace, fs...),
	}
}

// NewDataset creates a Dataset with the given name, namespace and optional facets.
func NewDataset(name, namespace string, datasetFacets ...facets.DatasetFacet) Dataset {
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
