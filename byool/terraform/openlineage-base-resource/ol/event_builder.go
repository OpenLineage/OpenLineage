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

// datasetFacetSelector is the minimal interface required by the dataset-facet building
// helpers. Both JobCapability and DatasetCapability satisfy it, so the helpers work
// for both job inputs/outputs and standalone dataset events without depending on the
// raw capability struct.
type datasetFacetSelector interface {
	IsDatasetEnabled(DatasetFacet) bool
}

// BuildRunEvent wraps a JobEvent with run-specific fields (event type + generated run ID).
// cap controls which job and dataset facets are emitted — disabled facets are skipped
// even if the corresponding model blocks are populated.
func BuildRunEvent(data *JobResourceModel, cap JobCapability) *openlineage.RunEvent {
	jobEvent := BuildJobEvent(data, cap)
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
// cap controls which job and dataset facets are emitted — disabled facets are skipped
// even if the corresponding model blocks are populated.
func BuildJobEvent(data *JobResourceModel, cap JobCapability) *openlineage.JobEvent {
	event := openlineage.NewJobEvent(
		data.Name.ValueString(),
		data.Namespace.ValueString(),
		producer,
	)

	if jfs := buildJobFacets(&data.OLJobConfig, cap); len(jfs) > 0 {
		event = event.WithFacets(jfs...)
	}

	for _, input := range data.Inputs {
		event = event.WithInputs(buildInputElement(&input, cap))
	}

	for _, output := range data.Outputs {
		event = event.WithOutputs(buildOutputElement(&output, cap))
	}

	return event
}

// buildJobFacets assembles all optional job facets from OLJobConfig.
// Only facets enabled in cap are emitted; disabled facets are skipped regardless
// of whether the model block is populated (stub values from portability schema).
func buildJobFacets(data *OLJobConfig, cap JobCapability) []facets.JobFacet {
	var fs []facets.JobFacet

	if cap.IsEnabled(FacetJobType) && data.JobType != nil {
		// integration and processing_type are Required in the schema, but guard
		// against null/unknown defensively (e.g. during import or plan phase).
		if data.JobType.Integration.IsNull() || data.JobType.Integration.IsUnknown() ||
			data.JobType.ProcessingType.IsNull() || data.JobType.ProcessingType.IsUnknown() {
			// skip — cannot build a valid JobTypeJobFacet without required fields
		} else {
			jt := facets.NewJobTypeJobFacet(
				producer,
				data.JobType.Integration.ValueString(),
				data.JobType.ProcessingType.ValueString(),
			)
			if !data.JobType.JobType.IsNull() && !data.JobType.JobType.IsUnknown() {
				jt = jt.WithJobType(data.JobType.JobType.ValueString())
			}
			fs = append(fs, jt)
		}
	}

	if cap.IsEnabled(FacetJobOwnership) && data.Ownership != nil && len(data.Ownership.Owners) > 0 {
		owners := make([]facets.Owner, 0, len(data.Ownership.Owners))
		for _, o := range data.Ownership.Owners {
			owner := facets.Owner{Name: o.Name.ValueString()}
			if !o.Type.IsNull() && !o.Type.IsUnknown() {
				owner.Type = openlineage.Ptr(o.Type.ValueString())
			}
			owners = append(owners, owner)
		}
		fs = append(fs, facets.NewOwnershipJobFacet(producer).WithOwners(owners))
	}

	if cap.IsEnabled(FacetJobDocumentation) && data.Documentation != nil {
		if !data.Documentation.Description.IsNull() && !data.Documentation.Description.IsUnknown() {
			fs = append(fs, facets.NewDocumentationJobFacet(
				producer,
				data.Documentation.Description.ValueString(),
			))
		}
	}

	if cap.IsEnabled(FacetJobSourceCode) && data.SourceCode != nil {
		if !data.SourceCode.Language.IsNull() && !data.SourceCode.Language.IsUnknown() &&
			!data.SourceCode.SourceCode.IsNull() && !data.SourceCode.SourceCode.IsUnknown() {
			fs = append(fs, facets.NewSourceCodeJobFacet(
				producer,
				data.SourceCode.Language.ValueString(),
				data.SourceCode.SourceCode.ValueString(),
			))
		}
	}

	if cap.IsEnabled(FacetJobSourceCodeLocation) && data.SourceCodeLocation != nil {
		// type and url are Required in the schema but can be Unknown at plan time
		// when derived from another resource — skip the facet rather than emitting
		// empty strings into the event.
		if !data.SourceCodeLocation.Type.IsNull() && !data.SourceCodeLocation.Type.IsUnknown() &&
			!data.SourceCodeLocation.URL.IsNull() && !data.SourceCodeLocation.URL.IsUnknown() {
			scl := facets.NewSourceCodeLocationJobFacet(
				producer,
				data.SourceCodeLocation.Type.ValueString(),
				data.SourceCodeLocation.URL.ValueString(),
			)
			if !data.SourceCodeLocation.RepoURL.IsNull() && !data.SourceCodeLocation.RepoURL.IsUnknown() {
				scl = scl.WithRepoURL(data.SourceCodeLocation.RepoURL.ValueString())
			}
			if !data.SourceCodeLocation.Path.IsNull() && !data.SourceCodeLocation.Path.IsUnknown() {
				scl = scl.WithPath(data.SourceCodeLocation.Path.ValueString())
			}
			if !data.SourceCodeLocation.Version.IsNull() && !data.SourceCodeLocation.Version.IsUnknown() {
				scl = scl.WithVersion(data.SourceCodeLocation.Version.ValueString())
			}
			if !data.SourceCodeLocation.Tag.IsNull() && !data.SourceCodeLocation.Tag.IsUnknown() {
				scl = scl.WithTag(data.SourceCodeLocation.Tag.ValueString())
			}
			if !data.SourceCodeLocation.Branch.IsNull() && !data.SourceCodeLocation.Branch.IsUnknown() {
				scl = scl.WithBranch(data.SourceCodeLocation.Branch.ValueString())
			}
			fs = append(fs, scl)
		}
	}

	if cap.IsEnabled(FacetJobSQL) && data.SQL != nil {
		if !data.SQL.Query.IsNull() && !data.SQL.Query.IsUnknown() {
			fs = append(fs, facets.NewSQLJobFacet(producer, data.SQL.Query.ValueString()))
		}
	}

	if cap.IsEnabled(FacetJobTags) && len(data.Tags) > 0 {
		tags := make([]facets.TagClass, 0, len(data.Tags))
		for _, t := range data.Tags {
			value := ""
			if !t.Value.IsNull() && !t.Value.IsUnknown() {
				value = t.Value.ValueString()
			}
			tags = append(tags, facets.TagClass{
				Key:   t.Name.ValueString(),
				Value: value,
			})
		}
		fs = append(fs, facets.NewTagsJobFacet(producer).WithTags(tags))
	}

	return fs
}

// BuildDatasetEvent assembles an OpenLineage DatasetEvent from the standalone dataset model.
// cap controls which dataset facets are emitted — disabled facets are skipped
// even if the corresponding model blocks are populated.
func BuildDatasetEvent(data *DatasetResourceModel, cap DatasetCapability) *openlineage.DatasetEvent {
	facetsList := buildDatasetFacets(&data.DatasetModel, cap)
	event := openlineage.NewDatasetEvent(
		data.Name.ValueString(),
		data.Namespace.ValueString(),
		producer,
		facetsList...,
	)

	return event
}

// buildInputElement converts a single Terraform InputModel into an OpenLineage InputElement.
func buildInputElement(input *OLInputModel, cap datasetFacetSelector) openlineage.InputElement {
	ie := openlineage.NewInputElement(
		input.Name.ValueString(),
		input.Namespace.ValueString(),
	)

	ie = ie.WithFacets(buildDatasetFacets(&input.DatasetModel, cap)...)

	return ie
}

// buildOutputElement converts a single Terraform OutputModel into an OpenLineage OutputElement.
func buildOutputElement(output *OLOutputModel, cap datasetFacetSelector) openlineage.OutputElement {
	oe := openlineage.NewOutputElement(
		output.Name.ValueString(),
		output.Namespace.ValueString(),
	)

	oe = oe.WithFacets(buildDatasetFacets(&output.DatasetModel, cap)...)

	if cap.IsDatasetEnabled(FacetDatasetColumnLineage) && output.ColumnLineage != nil {
		oe = oe.WithFacets(buildColumnLineageFacet(output.ColumnLineage))
	}

	return oe
}

func buildDatasetFacets(dataset *DatasetModel, cap datasetFacetSelector) []facets.DatasetFacet {
	var facetsList []facets.DatasetFacet

	if cap.IsDatasetEnabled(FacetDatasetSymlinks) && len(dataset.Symlinks) > 0 {
		facetsList = append(facetsList, buildSymlinksFacet(dataset.Symlinks))
	}

	if cap.IsDatasetEnabled(FacetDatasetSchema) && dataset.Schema != nil {
		facetsList = append(facetsList, buildSchemaFacet(dataset.Schema))
	}

	if cap.IsDatasetEnabled(FacetDatasetDataSource) && dataset.DataSource != nil {
		facetsList = append(facetsList, buildDataSourceFacet(dataset.DataSource))
	}

	if cap.IsDatasetEnabled(FacetDatasetDocumentation) && dataset.Documentation != nil {
		// description is Required in the schema but can be Unknown at plan time
		// when derived from another resource — skip rather than emitting an empty string.
		if !dataset.Documentation.Description.IsNull() && !dataset.Documentation.Description.IsUnknown() {
			facetsList = append(facetsList, facets.NewDocumentationDatasetFacet(
				producer,
				dataset.Documentation.Description.ValueString(),
			))
		}
	}

	if cap.IsDatasetEnabled(FacetDatasetType) && dataset.DatasetType != nil {
		// dataset_type is Required in the schema; guard against Unknown values at plan time.
		if !dataset.DatasetType.DatasetType.IsNull() && !dataset.DatasetType.DatasetType.IsUnknown() {
			dt := facets.NewDatasetTypeDatasetFacet(producer, dataset.DatasetType.DatasetType.ValueString())
			if !dataset.DatasetType.SubType.IsNull() && !dataset.DatasetType.SubType.IsUnknown() {
				dt = dt.WithSubType(dataset.DatasetType.SubType.ValueString())
			}
			facetsList = append(facetsList, dt)
		}
	}

	if cap.IsDatasetEnabled(FacetDatasetVersion) && dataset.Version != nil {
		// dataset_version is Required in the schema; guard against Unknown values at plan time.
		if !dataset.Version.DatasetVersion.IsNull() && !dataset.Version.DatasetVersion.IsUnknown() {
			facetsList = append(facetsList, facets.NewDatasetVersionDatasetFacet(
				producer,
				dataset.Version.DatasetVersion.ValueString(),
			))
		}
	}

	if cap.IsDatasetEnabled(FacetDatasetStorage) && dataset.Storage != nil {
		if sf := buildStorageFacet(dataset.Storage); sf != nil {
			facetsList = append(facetsList, sf)
		}
	}

	if cap.IsDatasetEnabled(FacetDatasetOwnership) && dataset.Ownership != nil && len(dataset.Ownership.Owners) > 0 {
		facetsList = append(facetsList, buildOwnershipDatasetFacet(dataset.Ownership))
	}

	if cap.IsDatasetEnabled(FacetDatasetLifecycleStateChange) && dataset.LifecycleStateChange != nil {
		facetsList = append(facetsList, buildLifecycleStateChangeFacet(dataset.LifecycleStateChange))
	}

	if cap.IsDatasetEnabled(FacetDatasetHierarchy) && dataset.Hierarchy != nil {
		facetsList = append(facetsList, buildHierarchyFacet(dataset.Hierarchy))
	}

	if cap.IsDatasetEnabled(FacetDatasetCatalog) && dataset.Catalog != nil {
		facetsList = append(facetsList, buildCatalogFacet(dataset.Catalog))
	}

	if cap.IsDatasetEnabled(FacetDatasetTags) && len(dataset.Tags) > 0 {
		facetsList = append(facetsList, buildTagsDatasetFacet(dataset.Tags))
	}

	return facetsList
}

// buildSchemaFacet creates a SchemaDatasetFacet from a SchemaDatasetModel.
func buildSchemaFacet(s *SchemaDatasetModel) *facets.SchemaDatasetFacet {
	fields := make([]facets.FieldElement, 0, len(s.Fields))
	for _, f := range s.Fields {
		fe := facets.FieldElement{Name: f.Name.ValueString()}
		if !f.Type.IsNull() && !f.Type.IsUnknown() {
			fe.Type = openlineage.Ptr(f.Type.ValueString())
		}
		if !f.Description.IsNull() && !f.Description.IsUnknown() {
			fe.Description = openlineage.Ptr(f.Description.ValueString())
		}
		fields = append(fields, fe)
	}
	return facets.NewSchemaDatasetFacet(producer).WithFields(fields)
}

// buildDataSourceFacet creates a DatasourceDatasetFacet from a DataSourceDatasetModel.
func buildDataSourceFacet(ds *DataSourceDatasetModel) *facets.DatasourceDatasetFacet {
	f := facets.NewDatasourceDatasetFacet(producer)
	if !ds.Name.IsNull() && !ds.Name.IsUnknown() {
		f = f.WithName(ds.Name.ValueString())
	}
	if !ds.URI.IsNull() && !ds.URI.IsUnknown() {
		f = f.WithURI(ds.URI.ValueString())
	}
	return f
}

// buildStorageFacet creates a StorageDatasetFacet from a StorageDatasetModel.
// Returns nil if storage_layer is null or unknown (e.g. during plan), so callers
// should check for nil and skip the facet rather than emitting an invalid event.
func buildStorageFacet(s *StorageDatasetModel) *facets.StorageDatasetFacet {
	if s.StorageLayer.IsNull() || s.StorageLayer.IsUnknown() {
		return nil
	}
	f := facets.NewStorageDatasetFacet(producer, s.StorageLayer.ValueString())
	if !s.FileFormat.IsNull() && !s.FileFormat.IsUnknown() {
		f = f.WithFileFormat(s.FileFormat.ValueString())
	}
	return f
}

// buildOwnershipDatasetFacet creates an OwnershipDatasetFacet from an OwnershipDatasetModel.
func buildOwnershipDatasetFacet(o *OwnershipDatasetModel) *facets.OwnershipDatasetFacet {
	owners := make([]facets.Owner, 0, len(o.Owners))
	for _, owner := range o.Owners {
		ow := facets.Owner{Name: owner.Name.ValueString()}
		if !owner.Type.IsNull() && !owner.Type.IsUnknown() {
			ow.Type = openlineage.Ptr(owner.Type.ValueString())
		}
		owners = append(owners, ow)
	}
	return facets.NewOwnershipDatasetFacet(producer).WithOwners(owners)
}

// buildLifecycleStateChangeFacet creates a LifecycleStateChangeDatasetFacet.
func buildLifecycleStateChangeFacet(lsc *LifecycleStateChangeDatasetModel) *facets.LifecycleStateChangeDatasetFacet {
	f := facets.NewLifecycleStateChangeDatasetFacet(
		producer,
		facets.LifecycleStateChangeEnum(lsc.LifecycleStateChange.ValueString()),
	)
	if lsc.PreviousIdentifier != nil {
		f = f.WithPreviousIdentifier(&facets.PreviousIdentifier{
			Name:      lsc.PreviousIdentifier.Name.ValueString(),
			Namespace: lsc.PreviousIdentifier.Namespace.ValueString(),
		})
	}
	return f
}

// buildHierarchyFacet creates a HierarchyDatasetFacet from a HierarchyDatasetModel.
//
// The OL HierarchyElement only carries Name and Type; Namespace from the
// model is not part of the spec and is intentionally dropped here.
// The hierarchy list is ordered highest → lowest: parent first, then children.
func buildHierarchyFacet(h *HierarchyDatasetModel) *facets.HierarchyDatasetFacet {
	elements := []facets.HierarchyElement{
		{Name: h.Parent.Name.ValueString(), Type: h.Parent.Type.ValueString()},
	}
	for _, child := range h.Children {
		elements = append(elements, facets.HierarchyElement{
			Name: child.Name.ValueString(),
			Type: child.Type.ValueString(),
		})
	}
	return facets.NewHierarchyDatasetFacet(producer).WithHierarchy(elements)
}

// buildTagsDatasetFacet creates a TagsDatasetFacet from a slice of TagsDatasetModel.
func buildTagsDatasetFacet(tags []TagsDatasetModel) *facets.TagsDatasetFacet {
	elements := make([]facets.TagElement, 0, len(tags))
	for _, t := range tags {
		elements = append(elements, facets.TagElement{
			Key:   t.Name.ValueString(),
			Value: t.Value.ValueString(),
		})
	}
	return facets.NewTagsDatasetFacet(producer).WithTags(elements)
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

			if inf.Transformation != nil {
				de.Transformations = []facets.Transformation{
					buildTransformation(inf.Transformation),
				}
			}

			inputFields = append(inputFields, de)
		}

		// The output column name is the map key — this is how OL consumers
		// look up "which inputs produced column X?"
		// If the same output column appears in multiple fields blocks, aggregate
		// their InputFields rather than silently overwriting the earlier entry.
		fieldName := f.Name.ValueString()
		if existing, ok := fields[fieldName]; ok {
			existing.InputFields = append(existing.InputFields, inputFields...)
			fields[fieldName] = existing
		} else {
			fields[fieldName] = facets.FieldValue{
				InputFields: inputFields,
			}
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

		if ds.Transformation != nil {
			de.Transformations = []facets.Transformation{
				buildTransformation(ds.Transformation),
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
