/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// ── primitive helpers ─────────────────────────────────────────────────────────

// str returns types.StringValue("test") when set, otherwise types.StringNull().
func str(set bool) types.String {
	if set {
		return types.StringValue("test")
	}
	return types.StringNull()
}

// bl returns types.BoolValue(true) when set, otherwise types.BoolNull().
func bl(set bool) types.Bool {
	if set {
		return types.BoolValue(true)
	}
	return types.BoolNull()
}

// ── event accessors ───────────────────────────────────────────────────────────

func jobFacets(event *openlineage.RunEvent) *facets.JobFacets {
	if event.Job.Facets == nil {
		return &facets.JobFacets{}
	}
	return event.Job.Facets
}

func inputFacets(event *openlineage.RunEvent, i int) *facets.DatasetFacets {
	if event.Inputs[i].Facets == nil {
		return &facets.DatasetFacets{}
	}
	return event.Inputs[i].Facets
}

func outputFacets(event *openlineage.RunEvent, i int) *facets.DatasetFacets {
	if event.Outputs[i].Facets == nil {
		return &facets.DatasetFacets{}
	}
	return event.Outputs[i].Facets
}

// ── capability builders ───────────────────────────────────────────────────────

func fullJobCapability() JobCapability {
	return EmptyJobCapability().
		WithFacetEnabled(
			FacetJobType, FacetJobOwnership, FacetJobDocumentation,
			FacetJobSourceCode, FacetJobSourceCodeLocation, FacetJobSQL, FacetJobTags,
		).
		WithDatasetFacetEnabled(
			FacetDatasetSymlinks, FacetDatasetSchema, FacetDatasetDataSource,
			FacetDatasetDocumentation, FacetDatasetType, FacetDatasetVersion,
			FacetDatasetStorage, FacetDatasetOwnership, FacetDatasetLifecycleStateChange,
			FacetDatasetHierarchy, FacetDatasetCatalog, FacetDatasetColumnLineage,
			FacetDatasetTags,
		)
}

// ── base job model builders ───────────────────────────────────────────────────

func minimalModel() *JobResourceModel {
	return &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("test-namespace"),
			Name:      types.StringValue("test-job"),
		},
	}
}

func simpleJobModel() *JobResourceModel {
	return &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("ns"),
			Name:      types.StringValue("job"),
		},
	}
}

// fullModel returns a JobResourceModel with every facet block populated (all
// required+optional fields set to "test"), used to verify disabled facets
// produce no output.
func fullModel() *JobResourceModel {
	return &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace:          types.StringValue("production"),
			Name:               types.StringValue("etl.orders.daily"),
			Description:        str(true),
			JobType:            jobTypeModel(true, true),
			Ownership:          ownershipJobModel(1, true, true),
			Documentation:      documentationModel(true, true),
			SourceCode:         sourceCodeModel(true),
			SourceCodeLocation: sourceCodeLocationModel(true, true),
			SQL:                sqlModel(true, true),
			Tags:               jobTagsModel(1, true, true),
		},
		Inputs: olInputs(func() DatasetModel {
			ds := datasetModel("hive", "raw.orders")
			ds.Symlinks = symlinksModel(1, true)
			ds.Schema = schemaModel(1, true, true)
			ds.DataSource = dataSourceModel(true)
			ds.Documentation = documentationModel(true, true)
			ds.DatasetType = datasetTypeModel(true, true)
			ds.Version = datasetVersionModel(true)
			ds.Storage = storageModel(true, true)
			ds.Ownership = datasetOwnershipModel(1, true, true)
			ds.LifecycleStateChange = lifecycleStateChangeModel(true, true)
			ds.Hierarchy = hierarchyModel(1, true)
			ds.Catalog = catalogModel(true, true)
			ds.Tags = datasetTagsModel(1, true, true)
			return ds
		}()),
		Outputs: []OLOutputModel{
			columnLineageOutput("bigquery", "warehouse.orders", "order_id", "hive", "raw.orders", "order_id",
				transformationModel(true, true)),
		},
	}
}

// ── job facet builders ────────────────────────────────────────────────────────

// jobTypeModel builds a JobTypeJobModel.
//
//	required → ProcessingType + Integration = "test" (both error on null)
//	optional → JobType = "test"
func jobTypeModel(required, optional bool) *JobTypeJobModel {
	return &JobTypeJobModel{
		ProcessingType: str(required),
		Integration:    str(required),
		JobType:        str(optional),
	}
}

// ownershipJobModel builds an OwnershipJobModel with n owners.
//
//	required → Name = "test"
//	optional → Type = "test"
func ownershipJobModel(n int, required, optional bool) *OwnershipJobModel {
	owners := make([]JobOwnerModel, n)
	for i := range owners {
		owners[i] = JobOwnerModel{Name: str(required), Type: str(optional)}
	}
	return &OwnershipJobModel{Owners: owners}
}

// documentationModel builds a DocumentationModel.
//
//	required → Description = "test"
//	optional → ContentType = "test"
func documentationModel(required, optional bool) *DocumentationModel {
	return &DocumentationModel{Description: str(required), ContentType: str(optional)}
}

// sourceCodeModel builds a SourceCodeJobModel.
//
//	required → Language + SourceCode = "test"
func sourceCodeModel(required bool) *SourceCodeJobModel {
	return &SourceCodeJobModel{Language: str(required), SourceCode: str(required)}
}

// sourceCodeLocationModel builds a SourceCodeLocationJobModel.
//
//	required → Type + URL = "test"
//	optional → RepoURL + Path + Version + Tag + Branch = "test"
func sourceCodeLocationModel(required, optional bool) *SourceCodeLocationJobModel {
	return &SourceCodeLocationJobModel{
		Type:    str(required),
		URL:     str(required),
		RepoURL: str(optional),
		Path:    str(optional),
		Version: str(optional),
		Tag:     str(optional),
		Branch:  str(optional),
	}
}

// sqlModel builds a SQLJobModel.
//
//	required → Query = "test"
//	optional → Dialect = "test"
func sqlModel(required, optional bool) *SQLJobModel {
	return &SQLJobModel{Query: str(required), Dialect: str(optional)}
}

// jobTagsModel builds a TagsJobFacetModel with n tag entries.
//
//	required → Name + Value = "test"
//	optional → Source = "test"
func jobTagsModel(n int, required, optional bool) *TagsJobFacetModel {
	tags := make([]TagsJobModel, n)
	for i := range tags {
		tags[i] = TagsJobModel{Name: str(required), Value: str(required), Source: str(optional)}
	}
	return &TagsJobFacetModel{Tag: tags}
}

// ── dataset facet builders ────────────────────────────────────────────────────

// symlinksModel builds a SymlinksDatasetFacetModel with n identifier entries.
//
//	required → Namespace + Name + Type = "test"
func symlinksModel(n int, required bool) *SymlinksDatasetFacetModel {
	ids := make([]IdentifierModel, n)
	for i := range ids {
		ids[i] = IdentifierModel{Namespace: str(required), Name: str(required), Type: str(required)}
	}
	return &SymlinksDatasetFacetModel{Identifier: ids}
}

// schemaModel builds a SchemaDatasetModel with n field entries.
//
//	required → Name = "test"
//	optional → Type + Description = "test"
func schemaModel(n int, required, optional bool) *SchemaDatasetModel {
	fields := make([]SchemaFieldModel, n)
	for i := range fields {
		fields[i] = SchemaFieldModel{Name: str(required), Type: str(optional), Description: str(optional)}
	}
	return &SchemaDatasetModel{Fields: fields}
}

// dataSourceModel builds a DataSourceDatasetModel.
//
//	required → Name + URI = "test"
func dataSourceModel(required bool) *DataSourceDatasetModel {
	return &DataSourceDatasetModel{Name: str(required), URI: str(required)}
}

// datasetTypeModel builds a DatasetTypeDatasetModel.
//
//	required → DatasetType = "test"
//	optional → SubType = "test"
func datasetTypeModel(required, optional bool) *DatasetTypeDatasetModel {
	return &DatasetTypeDatasetModel{DatasetType: str(required), SubType: str(optional)}
}

// datasetVersionModel builds a DatasetVersionDatasetModel.
//
//	required → DatasetVersion = "test"
func datasetVersionModel(required bool) *DatasetVersionDatasetModel {
	return &DatasetVersionDatasetModel{DatasetVersion: str(required)}
}

// storageModel builds a StorageDatasetModel.
//
//	required → StorageLayer = "test"
//	optional → FileFormat = "test"
func storageModel(required, optional bool) *StorageDatasetModel {
	return &StorageDatasetModel{StorageLayer: str(required), FileFormat: str(optional)}
}

// datasetOwnershipModel builds an OwnershipDatasetModel with n owner entries.
//
//	required → Name = "test"
//	optional → Type = "test"
func datasetOwnershipModel(n int, required, optional bool) *OwnershipDatasetModel {
	owners := make([]DatasetOwnerModel, n)
	for i := range owners {
		owners[i] = DatasetOwnerModel{Name: str(required), Type: str(optional)}
	}
	return &OwnershipDatasetModel{Owners: owners}
}

// lifecycleStateChangeModel builds a LifecycleStateChangeDatasetModel.
//
//	required → LifecycleStateChange = "test"
//	optional → PreviousIdentifier block with Namespace + Name = "test"
func lifecycleStateChangeModel(required, optional bool) *LifecycleStateChangeDatasetModel {
	m := &LifecycleStateChangeDatasetModel{LifecycleStateChange: str(required)}
	if optional {
		m.PreviousIdentifier = &PreviousIdentifierModel{
			Namespace: str(true),
			Name:      str(true),
		}
	}
	return m
}

// hierarchyModel builds a HierarchyDatasetModel with n element entries.
//
//	required → Name + Type = "test"
func hierarchyModel(n int, required bool) *HierarchyDatasetModel {
	elems := make([]HierarchyElementModel, n)
	for i := range elems {
		elems[i] = HierarchyElementModel{Name: str(required), Type: str(required)}
	}
	return &HierarchyDatasetModel{Hierarchy: elems}
}

// catalogModel builds a CatalogDatasetModel.
//
//	required → Framework + Type + Name = "test"
//	optional → MetadataURI + WarehouseURI + Source = "test" (CatalogProperties omitted for simplicity)
func catalogModel(required, optional bool) *CatalogDatasetModel {
	return &CatalogDatasetModel{
		Framework:    str(required),
		Type:         str(required),
		Name:         str(required),
		MetadataURI:  str(optional),
		WarehouseURI: str(optional),
		Source:       str(optional),
	}
}

// datasetTagsModel builds a TagsDatasetFacetModel with n tag entries.
//
//	required → Name + Value = "test"
//	optional → Source + Field = "test"
func datasetTagsModel(n int, required, optional bool) *TagsDatasetFacetModel {
	tags := make([]TagsDatasetModel, n)
	for i := range tags {
		tags[i] = TagsDatasetModel{
			Name:   str(required),
			Value:  str(required),
			Source: str(optional),
			Field:  str(optional),
		}
	}
	return &TagsDatasetFacetModel{Tag: tags}
}

// ── column lineage builders ───────────────────────────────────────────────────

// inputFieldModel builds an InputFieldModel.
//
//	required → Namespace + Name + Field = "test"
//	optional → nTransforms transformation entries (transformationModel(required, optional))
func inputFieldModel(nTransforms int, required, optional bool) InputFieldModel {
	transforms := make([]TransformationModel, nTransforms)
	for i := range transforms {
		transforms[i] = transformationModel(required, optional)
	}
	return InputFieldModel{
		Namespace:       str(required),
		Name:            str(required),
		Field:           str(required),
		Transformations: transforms,
	}
}

// columnLineageFieldModel builds a ColumnLineageFieldModel.
//
//	required → Name = "test"; each input field: Namespace + Name + Field = "test"
//	optional → each input field includes nTransforms transformation entries
func columnLineageFieldModel(nInputs, nTransforms int, required, optional bool) ColumnLineageFieldModel {
	inputs := make([]InputFieldModel, nInputs)
	for i := range inputs {
		inputs[i] = inputFieldModel(nTransforms, required, optional)
	}
	return ColumnLineageFieldModel{Name: str(required), InputFields: inputs}
}

// columnLineageDatasetElementModel builds a ColumnLineageDatasetElementModel.
//
//	required → Namespace + Name + Field = "test"
//	optional → nTransforms transformation entries
func columnLineageDatasetElementModel(nTransforms int, required, optional bool) ColumnLineageDatasetElementModel {
	transforms := make([]TransformationModel, nTransforms)
	for i := range transforms {
		transforms[i] = transformationModel(required, optional)
	}
	return ColumnLineageDatasetElementModel{
		Namespace:       str(required),
		Name:            str(required),
		Field:           str(required),
		Transformations: transforms,
	}
}

// columnLineageModel builds a ColumnLineageDatasetModel.
//
//	nFields:   number of output field mappings
//	nInputs:   number of input fields per output field
//	nTransforms: number of transformations per input/dataset element
//	nDataset:  number of dataset-level elements
//	required → all Name/Namespace/Field values = "test"
//	optional → transformation entries included
func columnLineageModel(nFields, nInputs, nTransforms, nDataset int, required, optional bool) *ColumnLineageDatasetModel {
	fields := make([]ColumnLineageFieldModel, nFields)
	for i := range fields {
		fields[i] = columnLineageFieldModel(nInputs, nTransforms, required, optional)
	}
	dataset := make([]ColumnLineageDatasetElementModel, nDataset)
	for i := range dataset {
		dataset[i] = columnLineageDatasetElementModel(nTransforms, required, optional)
	}
	return &ColumnLineageDatasetModel{Fields: fields, Dataset: dataset}
}

// ── transformation builder ────────────────────────────────────────────────────

// transformationModel builds a TransformationModel.
//
//	required → Type = "test"
//	optional → Subtype + Description = "test", Masking = true
func transformationModel(required, optional bool) TransformationModel {
	return TransformationModel{
		Type:        str(required),
		Subtype:     str(optional),
		Description: str(optional),
		Masking:     bl(optional),
	}
}

// ── dataset / IO builders ─────────────────────────────────────────────────────

func datasetModel(ns, name string) DatasetModel {
	return DatasetModel{
		Namespace: types.StringValue(ns),
		Name:      types.StringValue(name),
	}
}

func olInputs(datasets ...DatasetModel) []OLInputModel {
	inputs := make([]OLInputModel, len(datasets))
	for i, ds := range datasets {
		inputs[i] = OLInputModel{DatasetModel: ds}
	}
	return inputs
}

func olOutputs(datasets ...DatasetModel) []OLOutputModel {
	outputs := make([]OLOutputModel, len(datasets))
	for i, ds := range datasets {
		outputs[i] = OLOutputModel{DatasetModel: ds}
	}
	return outputs
}

func columnLineageOutput(outputNs, outputName, outputCol, inputNs, inputName, inputField string, transforms ...TransformationModel) OLOutputModel {
	return OLOutputModel{
		DatasetModel: datasetModel(outputNs, outputName),
		ColumnLineage: &ColumnLineageDatasetModel{
			Fields: []ColumnLineageFieldModel{
				{
					Name: types.StringValue(outputCol),
					InputFields: []InputFieldModel{
						{
							Namespace:       types.StringValue(inputNs),
							Name:            types.StringValue(inputName),
							Field:           types.StringValue(inputField),
							Transformations: transforms,
						},
					},
				},
			},
		},
	}
}

// ── tag string-pair helpers (kept for tests that exercise null-entry skipping) ─

// jobTags builds a TagsJobFacetModel from name/value string pairs.
// An empty string "" is treated as types.StringNull().
func jobTags(pairs ...string) *TagsJobFacetModel {
	toStr := func(s string) types.String {
		if s == "" {
			return types.StringNull()
		}
		return types.StringValue(s)
	}
	tags := make([]TagsJobModel, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		tags = append(tags, TagsJobModel{Name: toStr(pairs[i]), Value: toStr(pairs[i+1])})
	}
	return &TagsJobFacetModel{Tag: tags}
}

// datasetTags builds a TagsDatasetFacetModel from name/value/field string triplets.
// An empty string "" is treated as types.StringNull().
func datasetTags(triplets ...string) *TagsDatasetFacetModel {
	toStr := func(s string) types.String {
		if s == "" {
			return types.StringNull()
		}
		return types.StringValue(s)
	}
	tags := make([]TagsDatasetModel, 0, len(triplets)/3)
	for i := 0; i+2 < len(triplets); i += 3 {
		tags = append(tags, TagsDatasetModel{
			Name:  toStr(triplets[i]),
			Value: toStr(triplets[i+1]),
			Field: toStr(triplets[i+2]),
		})
	}
	return &TagsDatasetFacetModel{Tag: tags}
}
