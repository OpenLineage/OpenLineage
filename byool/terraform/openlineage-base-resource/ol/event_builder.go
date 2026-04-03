/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/google/uuid"
)

// producer is the URI that identifies this provider as the source of OL events.
// In OpenLineage, every event carries a "producer" field so consumers know
// which system generated the lineage data.
const producer = "https://github.com/OpenLineage/openlineage/byool/terraform"

// BuildRunEvent wraps a JobEvent with run-specific fields (event type + generated run ID).
func BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent {
	jobEvent := BuildJobEvent(data)
	runID := uuid.New()

	return &openlineage.RunEvent{
		BaseEvent: openlineage.BaseEvent{
			Producer:  producer,
			SchemaURL: openlineage.RunEventSchemaURL,
			EventTime: jobEvent.EventTime,
		},
		Run: openlineage.RunInfo{
			RunID: runID.String(),
		},
		Job:       jobEvent.Job,
		EventType: openlineage.EventTypeComplete,
		Inputs:    jobEvent.Inputs,
		Outputs:   jobEvent.Outputs,
	}
}

// BuildJobEvent assembles an OpenLineage JobEvent from the Terraform job model.
func BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent {
	event := openlineage.NewJobEvent(
		data.Name.ValueString(),
		data.Namespace.ValueString(),
		producer,
	)

	for _, input := range data.Inputs {
		event = event.WithInputs(buildInputElement(&input))
	}

	for _, output := range data.Outputs {
		event = event.WithOutputs(buildOutputElement(&output))
	}

	return event
}

// BuildDatasetEvent assembles an OpenLineage DatasetEvent from the standalone dataset model.
func BuildDatasetEvent(data *DatasetResourceModel) *openlineage.DatasetEvent {
	facetsList := buildDatasetFacets(&data.DatasetModel)
	event := openlineage.NewDatasetEvent(
		data.Name.ValueString(),
		data.Namespace.ValueString(),
		producer,
		facetsList...,
	)

	return event
}

// buildInputElement converts a single Terraform InputModel into an OpenLineage InputElement.
//
// An InputElement = dataset identity (name + namespace) + optional dataset facets.
// Facets are additional metadata blobs attached to the dataset — they're optional
// and each facet type is independent (you can have symlinks without catalog, etc.).
func buildInputElement(input *OLInputModel) openlineage.InputElement {
	ie := openlineage.NewInputElement(
		input.Name.ValueString(),      // promoted from embedded DatasetModel
		input.Namespace.ValueString(), // promoted from embedded DatasetModel
	)

	ie = ie.WithFacets(buildDatasetFacets(&input.DatasetModel)...)

	return ie
}

// buildOutputElement converts a single Terraform OutputModel into an OpenLineage OutputElement.
//
// Same as buildInputElement but outputs can additionally carry column-level lineage,
// which describes exactly which input columns produced which output columns.
func buildOutputElement(output *OLOutputModel) openlineage.OutputElement {
	oe := openlineage.NewOutputElement(
		output.Name.ValueString(),      // promoted from embedded DatasetModel
		output.Namespace.ValueString(), // promoted from embedded DatasetModel
	)

	oe = oe.WithFacets(buildDatasetFacets(&output.DatasetModel)...)

	// Column lineage is only meaningful on outputs — it maps output columns
	// back to the input columns that produced them.
	if output.ColumnLineage != nil {
		oe = oe.WithFacets(buildColumnLineageFacet(output.ColumnLineage))
	}

	return oe
}

func buildDatasetFacets(dataset *DatasetModel) []facets.DatasetFacet {
	var facetsList []facets.DatasetFacet

	if len(dataset.Symlinks) > 0 {
		facetsList = append(facetsList, buildSymlinksFacet(dataset.Symlinks))
	}

	if dataset.Catalog != nil {
		facetsList = append(facetsList, buildCatalogFacet(dataset.Catalog))
	}

	return facetsList
}

// buildSymlinksFacet creates a SymlinksDatasetFacet from a list of SymlinkModels.
//
// A symlink says: "this dataset can also be found at this other name/namespace".
// Useful when the same physical table is registered in multiple catalogs
// (e.g. the same data appears in both BigQuery and Hive).
func buildSymlinksFacet(symlinks []SymlinksDatasetModel) *facets.SymlinksDatasetFacet {
	identifiers := make([]facets.Identifier, 0, len(symlinks))
	for _, s := range symlinks {
		identifiers = append(identifiers, facets.Identifier{
			Name:      s.Name.ValueString(),
			Namespace: s.Namespace.ValueString(),
			Type:      s.Type.ValueString(), // e.g. "TABLE"
		})
	}
	return facets.NewSymlinksDatasetFacet(producer).WithIdentifiers(identifiers)
}

// buildCatalogFacet creates a CatalogDatasetFacet from a CatalogModel.
//
// This facet tells consumers where to find the dataset's schema/metadata:
// which metastore (framework), what type, and the URIs to reach it.
// Optional fields (MetadataURI, WarehouseURI, Source) are only set if
// the user provided them — we guard with IsNull/IsUnknown to avoid
// sending empty strings that would override valid defaults.
func buildCatalogFacet(c *CatalogDatasetModel) *facets.CatalogDatasetFacet {
	cat := facets.NewCatalogDatasetFacet(
		producer,
		c.Framework.ValueString(),
		c.Type.ValueString(),
		c.Name.ValueString(),
	)

	// IsNull() → the attribute was not set in the config at all
	// IsUnknown() → the value isn't known yet at plan time (computed from another resource)
	// We only call the setter if we actually have a real value.
	if !c.MetadataURI.IsNull() && !c.MetadataURI.IsUnknown() {
		cat = cat.WithMetadataURI(c.MetadataURI.ValueString())
	}
	if !c.WarehouseURI.IsNull() && !c.WarehouseURI.IsUnknown() {
		cat = cat.WithWarehouseURI(c.WarehouseURI.ValueString())
	}
	if !c.Source.IsNull() && !c.Source.IsUnknown() {
		cat = cat.WithSource(c.Source.ValueString())
	}

	return cat
}

// buildColumnLineageFacet creates a ColumnLineageFacet from the Terraform column_lineage blocks.
//
// The OL ColumnLineageFacet has two parts:
//
//  1. Fields map[string]FieldValue — keyed by output column name.
//     Each FieldValue contains a list of input DatasetElements (which input
//     dataset + column contributed to this output column, and how).
//
//  2. Dataset []DatasetElement — dataset-level lineage where we know the input
//     dataset contributed to an output field but don't know the exact column.
//
// The Terraform schema mirrors this: `fields {}` blocks → Fields map,
// `dataset {}` blocks → Dataset slice.
func buildColumnLineageFacet(clm *ColumnLineageDatasetModel) *facets.ColumnLineageDatasetFacet {
	// fields will be keyed by output column name, matching the OL spec.
	fields := make(map[string]facets.FieldValue)
	var datasetElements []facets.DatasetElement

	// ── fields blocks → Fields map ─────────────────────────────────────
	for _, f := range clm.Fields {
		var inputFields []facets.DatasetElement

		// Each `input_field {}` sub-block is one DatasetElement:
		// the input dataset (name+namespace) and the specific column (field)
		// that contributed to this output column.
		for _, inf := range f.InputFields {
			de := facets.DatasetElement{
				Name:      inf.Name.ValueString(),
				Namespace: inf.Namespace.ValueString(),
				Field:     inf.Field.ValueString(),
			}

			// Transformation is optional — only attach if the user declared it.
			// We use index [0] because only one transformation per input field
			// makes sense in the OL spec.
			if len(inf.Transformation) > 0 {
				de.Transformations = []facets.Transformation{
					buildTransformation(&inf.Transformation[0]),
				}
			}

			inputFields = append(inputFields, de)
		}

		// The output column name is the map key — this is how OL consumers
		// look up "which inputs produced column X?"
		fields[f.Name.ValueString()] = facets.FieldValue{
			InputFields: inputFields,
		}
	}

	// ── dataset blocks → DatasetElement slice ─────────────────────────
	// Dataset-level lineage: we know input dataset X contributed to output
	// field Y, but we don't know the exact input column.
	for _, ds := range clm.Dataset {
		de := facets.DatasetElement{
			Name:      ds.Name.ValueString(),
			Namespace: ds.Namespace.ValueString(),
			Field:     ds.Field.ValueString(),
		}

		if len(ds.Transformation) > 0 {
			de.Transformations = []facets.Transformation{
				buildTransformation(&ds.Transformation[0]),
			}
		}

		datasetElements = append(datasetElements, de)
	}

	cl := facets.NewColumnLineageDatasetFacet(producer).WithFields(fields)

	// WithDataset is only called if there are dataset-level entries —
	// the OL client may treat an empty slice differently from no call at all.
	if len(datasetElements) > 0 {
		cl = cl.WithDataset(datasetElements)
	}
	return cl
}

// buildTransformation converts a Terraform TransformationModel to the OL Transformation type.
//
// Transformation describes *how* data flowed from input to output:
//   - DIRECT: the output field is derived directly from the input (e.g. copied, cast)
//   - INDIRECT: the output field is influenced by the input but not a direct copy
//     (e.g. a filter condition, an aggregation key)
//
// Subtype, Description, and Masking are optional — we only set them if the user
// provided non-null values. openlineage.Ptr() wraps a value in a pointer,
// which is how the OL spec marks optional scalar fields.
func buildTransformation(t *TransformationModel) facets.Transformation {
	tr := facets.Transformation{
		Type: t.Type.ValueString(), // "DIRECT" or "INDIRECT" — always required
	}

	if !t.Subtype.IsNull() && !t.Subtype.IsUnknown() {
		tr.Subtype = openlineage.Ptr(t.Subtype.ValueString()) // e.g. "IDENTITY", "FILTER"
	}

	if !t.Description.IsNull() && !t.Description.IsUnknown() {
		tr.Description = openlineage.Ptr(t.Description.ValueString())
	}

	if !t.Masking.IsNull() && !t.Masking.IsUnknown() {
		tr.Masking = openlineage.Ptr(t.Masking.ValueBool()) // true = this transform masks PII etc.
	}

	return tr
}
