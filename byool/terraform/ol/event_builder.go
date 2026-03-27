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

// producer is the URI that identifies this dataplex as the source of OL events.
// In OpenLineage, every event carries a "producer" field so consumers know
// which system generated the lineage data.
const producer = "https://github.com/tomasznazarewicz/openlineage-terraform-provider"

// ProviderOriginName is placed in the GcpLineage facet's Origin.Name field.
// Exported so consumer packages can verify origin on Read.
const ProviderOriginName = "openlineage-byol-dataplex-v1.46.0-prerelease"

// BuildRunEvent is the top-level function that assembles a complete OpenLineage RunEvent
// from the Terraform resource model. The result is passed to emitAndCapture().
//
// An OpenLineage RunEvent has:
//   - EventType: always COMPLETE here (we declare the job finished successfully)
//   - Run: identified by a UUID we generate fresh on every apply
//   - Job: identified by name + namespace from the Terraform config
//   - Inputs/Outputs: datasets the job reads from / writes to, each with optional facets
func BuildRunEvent(data *JobResourceModel, runID uuid.UUID) *openlineage.RunEvent {
	// NewNamespacedRunEvent creates the base event with the job identity baked in.
	// The runID UUID is unique per apply — each terraform apply creates a new Run
	// in Dataplex even if the job config hasn't changed.
	event := openlineage.NewNamespacedRunEvent(
		openlineage.EventTypeComplete, // always COMPLETE — we declare success
		runID,                         // fresh UUID generated in Create/Update
		data.Name.ValueString(),       // promoted from embedded OLJobConfig
		data.Namespace.ValueString(),  // promoted from embedded OLJobConfig
		producer,                      // who created this event
	)

	// Each `inputs {}` block in the Terraform config becomes one InputElement.
	// The OL spec uses inputs to describe datasets the job READ from.
	for _, input := range data.Inputs {
		ie := buildInputElement(&input)
		event = event.WithInputs(ie)
	}

	// Each `outputs {}` block becomes one OutputElement.
	// The OL spec uses outputs to describe datasets the job WROTE to.
	for _, output := range data.Outputs {
		oe := buildOutputElement(&output)
		event = event.WithOutputs(oe)
	}

	event = event.WithJobFacets(facets.NewGcpLineageJobFacet(producer).WithOrigin(&facets.Origin{
		Name:       openlineage.Ptr(ProviderOriginName),
		SourceType: openlineage.Ptr("CUSTOM"),
	}))

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

	// Symlinks facet: lets consumers find this dataset under alternate names.
	// Only attached if the user declared at least one `symlinks {}` block.
	if len(input.Symlinks) > 0 {
		ie = ie.WithFacets(buildSymlinksFacet(input.Symlinks))
	}

	// Catalog facet: metastore/catalog metadata (Hive, Iceberg, etc.).
	// Only attached if the user declared a `catalog {}` block.
	// We use index [0] because only one catalog facet makes sense per dataset.
	if input.Catalog != nil {
		ie = ie.WithFacets(buildCatalogFacet(input.Catalog))
	}

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

	if len(output.Symlinks) > 0 {
		oe = oe.WithFacets(buildSymlinksFacet(output.Symlinks))
	}

	if output.Catalog != nil {
		oe = oe.WithFacets(buildCatalogFacet(output.Catalog))
	}

	// Column lineage is only meaningful on outputs — it maps output columns
	// back to the input columns that produced them.
	if output.ColumnLineage != nil {
		oe = oe.WithFacets(buildColumnLineageFacet(output.ColumnLineage))
	}

	return oe
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
