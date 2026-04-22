/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"fmt"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// producer is the URI that identifies this provider as the source of OL events.
// In OpenLineage, every event carries a "producer" field so consumers know
// which system generated the lineage data.
const producer = "https://github.com/OpenLineage/openlineage/byool/terraform"

// OpenLineageEventBuilder assembles OpenLineage events from Terraform models.
// All build methods are receivers so they share the Diagnostics sink and capability,
// and can attach precise path-aware errors for missing required attributes.
type OpenLineageEventBuilder struct {
	Diagnostics *diag.Diagnostics
	cap         capability
}

// NewJobEventBuilder creates a builder for job events (and their input/output datasets).
func NewJobEventBuilder(diags *diag.Diagnostics, cap JobCapability) *OpenLineageEventBuilder {
	return &OpenLineageEventBuilder{Diagnostics: diags, cap: cap.capability}
}

// NewDatasetEventBuilder creates a builder for standalone dataset events.
func NewDatasetEventBuilder(diags *diag.Diagnostics, cap DatasetCapability) *OpenLineageEventBuilder {
	return &OpenLineageEventBuilder{Diagnostics: diags, cap: cap.capability}
}

// ── Package-level convenience wrappers ───────────────────────────────────────

// BuildRunEvent is a package-level convenience wrapper around
// NewJobEventBuilder(...).BuildRunEvent(...).
func BuildRunEvent(data *JobResourceModel, cap JobCapability) *openlineage.RunEvent {
	var diags diag.Diagnostics
	return NewJobEventBuilder(&diags, cap).BuildRunEvent(data)
}

// BuildDatasetEvent is a package-level convenience wrapper around
// NewDatasetEventBuilder(...).BuildDatasetEvent(...).
func BuildDatasetEvent(data *DatasetResourceModel, cap DatasetCapability) *openlineage.DatasetEvent {
	var diags diag.Diagnostics
	return NewDatasetEventBuilder(&diags, cap).BuildDatasetEvent(data)
}

// ── Top-level event builders ──────────────────────────────────────────────────

// BuildRunEvent wraps a JobEvent with run-specific fields (event type + run ID).
func (e *OpenLineageEventBuilder) BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent {
	jobEvent := e.BuildJobEvent(data)
	runID := uuid.New()

	return &openlineage.RunEvent{
		BaseEvent: openlineage.BaseEvent{
			Producer:  producer,
			SchemaURL: openlineage.RunEventSchemaURL,
			EventTime: jobEvent.EventTime,
		},
		Run:       openlineage.RunInfo{RunID: runID.String()},
		Job:       jobEvent.Job,
		EventType: openlineage.EventTypeComplete,
		Inputs:    jobEvent.Inputs,
		Outputs:   jobEvent.Outputs,
	}
}

// BuildJobEvent assembles an OpenLineage JobEvent from the Terraform job model.
func (e *OpenLineageEventBuilder) BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent {
	base := path.Empty()

	event := openlineage.NewJobEvent(
		e.requireString(data.Name, base.AtName("name")),
		e.requireString(data.Namespace, base.AtName("namespace")),
		producer,
	)

	if jfs := e.buildJobFacets(&data.OLJobConfig, base); len(jfs) > 0 {
		event = event.WithFacets(jfs...)
	}

	for i, input := range data.Inputs {
		event = event.WithInputs(e.buildInputElement(&input, base.AtName("inputs").AtListIndex(i)))
	}

	for i, output := range data.Outputs {
		event = event.WithOutputs(e.buildOutputElement(&output, base.AtName("outputs").AtListIndex(i)))
	}

	return event
}

// BuildDatasetEvent assembles an OpenLineage DatasetEvent from the standalone dataset model.
func (e *OpenLineageEventBuilder) BuildDatasetEvent(data *DatasetResourceModel) *openlineage.DatasetEvent {
	base := path.Empty()
	facetsList := e.buildDatasetFacets(&data.DatasetModel, base)
	return openlineage.NewDatasetEvent(
		e.requireString(data.Name, base.AtName("name")),
		e.requireString(data.Namespace, base.AtName("namespace")),
		producer,
		facetsList...,
	)
}

// ── Job facets ────────────────────────────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildJobFacets(data *OLJobConfig, base path.Path) []facets.JobFacet {
	var fs []facets.JobFacet

	if e.cap.isJobEnabled(FacetJobType) && data.JobType != nil &&
		isKnownString(data.JobType.Integration) && isKnownString(data.JobType.ProcessingType) {
		fs = append(fs, e.buildJobTypeFacet(data.JobType, base.AtName("job_type")))
	}
	if e.cap.isJobEnabled(FacetJobOwnership) && data.Ownership != nil {
		fs = append(fs, e.buildOwnershipJobFacet(data.Ownership, base.AtName("ownership")))
	}
	if e.cap.isJobEnabled(FacetJobDocumentation) && data.Documentation != nil &&
		isKnownString(data.Documentation.Description) {
		fs = append(fs, e.buildDocumentationJobFacet(data.Documentation, base.AtName("documentation")))
	}
	if e.cap.isJobEnabled(FacetJobSourceCode) && data.SourceCode != nil {
		fs = append(fs, e.buildSourceCodeJobFacet(data.SourceCode, base.AtName("source_code")))
	}
	if e.cap.isJobEnabled(FacetJobSourceCodeLocation) && data.SourceCodeLocation != nil &&
		isKnownString(data.SourceCodeLocation.Type) {
		fs = append(fs, e.buildSourceCodeLocationJobFacet(data.SourceCodeLocation, base.AtName("source_code_location")))
	}
	if e.cap.isJobEnabled(FacetJobSQL) && data.SQL != nil {
		fs = append(fs, e.buildSQLJobFacet(data.SQL, base.AtName("sql")))
	}
	if e.cap.isJobEnabled(FacetJobTags) && len(data.Tags) > 0 {
		if f := e.buildTagsJobFacet(data.Tags, base.AtName("tags")); f != nil {
			fs = append(fs, f)
		}
	}

	return fs
}

func (e *OpenLineageEventBuilder) buildJobTypeFacet(data *JobTypeJobModel, base path.Path) *facets.JobTypeJobFacet {
	jt := facets.NewJobTypeJobFacet(
		producer,
		e.requireString(data.Integration, base.AtName("integration")),
		e.requireString(data.ProcessingType, base.AtName("processing_type")),
	)
	jt.JobType = stringPtrIfKnown(data.JobType)
	return jt
}

func (e *OpenLineageEventBuilder) buildOwnershipJobFacet(data *OwnershipJobModel, base path.Path) *facets.OwnershipJobFacet {
	owners := make([]facets.Owner, 0, len(data.Owners))
	for i, o := range data.Owners {
		owners = append(owners, e.buildOwner(o.Name, o.Type, base.AtName("owners").AtListIndex(i)))
	}
	return facets.NewOwnershipJobFacet(producer).WithOwners(owners)
}

func (e *OpenLineageEventBuilder) buildDocumentationJobFacet(data *DocumentationModel, base path.Path) *facets.DocumentationJobFacet {
	df := facets.NewDocumentationJobFacet(producer, e.requireString(data.Description, base.AtName("description")))
	df.ContentType = stringPtrIfKnown(data.ContentType)
	return df
}

func (e *OpenLineageEventBuilder) buildSourceCodeJobFacet(data *SourceCodeJobModel, base path.Path) *facets.SourceCodeJobFacet {
	return facets.NewSourceCodeJobFacet(
		producer,
		e.requireString(data.Language, base.AtName("language")),
		e.requireString(data.SourceCode, base.AtName("source_code")),
	)
}

func (e *OpenLineageEventBuilder) buildSourceCodeLocationJobFacet(data *SourceCodeLocationJobModel, base path.Path) *facets.SourceCodeLocationJobFacet {
	scl := facets.NewSourceCodeLocationJobFacet(
		producer,
		e.requireString(data.Type, base.AtName("type")),
		e.requireString(data.URL, base.AtName("url")),
	)
	scl.RepoURL = stringPtrIfKnown(data.RepoURL)
	scl.Path = stringPtrIfKnown(data.Path)
	scl.Version = stringPtrIfKnown(data.Version)
	scl.Tag = stringPtrIfKnown(data.Tag)
	scl.Branch = stringPtrIfKnown(data.Branch)
	return scl
}

func (e *OpenLineageEventBuilder) buildSQLJobFacet(data *SQLJobModel, base path.Path) *facets.SQLJobFacet {
	sf := facets.NewSQLJobFacet(producer, e.requireString(data.Query, base.AtName("query")))
	if isKnownString(data.Dialect) {
		sf = sf.WithDialect(data.Dialect.ValueString())
	}
	return sf
}

func (e *OpenLineageEventBuilder) buildTagsJobFacet(tags []TagsJobModel, base path.Path) *facets.TagsJobFacet {
	ts := make([]facets.TagClass, 0, len(tags))
	for i, t := range tags {
		if !isKnownString(t.Name) || !isKnownString(t.Value) {
			continue
		}
		tp := base.AtListIndex(i)
		tc := facets.TagClass{
			Key:   e.requireString(t.Name, tp.AtName("name")),
			Value: e.requireString(t.Value, tp.AtName("value")),
		}
		tc.Source = stringPtrIfKnown(t.Source)
		ts = append(ts, tc)
	}
	if len(ts) == 0 {
		return nil
	}
	return facets.NewTagsJobFacet(producer).WithTags(ts)
}

// ── Dataset / input / output builders ────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildInputElement(input *OLInputModel, base path.Path) openlineage.InputElement {
	ie := openlineage.NewInputElement(
		e.requireString(input.Name, base.AtName("name")),
		e.requireString(input.Namespace, base.AtName("namespace")),
	)
	ie = ie.WithFacets(e.buildDatasetFacets(&input.DatasetModel, base)...)
	return ie
}

func (e *OpenLineageEventBuilder) buildOutputElement(output *OLOutputModel, base path.Path) openlineage.OutputElement {
	oe := openlineage.NewOutputElement(
		e.requireString(output.Name, base.AtName("name")),
		e.requireString(output.Namespace, base.AtName("namespace")),
	)
	oe = oe.WithFacets(e.buildDatasetFacets(&output.DatasetModel, base)...)

	if e.cap.isDatasetEnabled(FacetDatasetColumnLineage) && output.ColumnLineage != nil {
		oe = oe.WithFacets(e.buildColumnLineageFacet(output.ColumnLineage, base.AtName("column_lineage")))
	}
	return oe
}

func (e *OpenLineageEventBuilder) buildDatasetFacets(dataset *DatasetModel, base path.Path) []facets.DatasetFacet {
	var fs []facets.DatasetFacet

	if e.cap.isDatasetEnabled(FacetDatasetSymlinks) && len(dataset.Symlinks) > 0 {
		fs = append(fs, e.buildSymlinksFacet(dataset.Symlinks, base.AtName("symlinks")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetSchema) && dataset.Schema != nil {
		fs = append(fs, e.buildSchemaFacet(dataset.Schema, base.AtName("schema")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetDataSource) && dataset.DataSource != nil {
		fs = append(fs, e.buildDataSourceFacet(dataset.DataSource, base.AtName("data_source")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetDocumentation) && dataset.Documentation != nil {
		fs = append(fs, e.buildDocumentationDatasetFacet(dataset.Documentation, base.AtName("documentation")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetType) && dataset.DatasetType != nil {
		fs = append(fs, e.buildDatasetTypeFacet(dataset.DatasetType, base.AtName("dataset_type")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetVersion) && dataset.Version != nil {
		fs = append(fs, e.buildDatasetVersionFacet(dataset.Version, base.AtName("version")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetStorage) && dataset.Storage != nil &&
		isKnownString(dataset.Storage.StorageLayer) {
		fs = append(fs, e.buildStorageFacet(dataset.Storage, base.AtName("storage")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetOwnership) && dataset.Ownership != nil {
		fs = append(fs, e.buildOwnershipDatasetFacet(dataset.Ownership, base.AtName("ownership")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetLifecycleStateChange) && dataset.LifecycleStateChange != nil {
		fs = append(fs, e.buildLifecycleStateChangeFacet(dataset.LifecycleStateChange, base.AtName("lifecycle_state_change")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetHierarchy) && dataset.Hierarchy != nil {
		fs = append(fs, e.buildHierarchyFacet(dataset.Hierarchy, base.AtName("hierarchy")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetCatalog) && dataset.Catalog != nil &&
		isKnownString(dataset.Catalog.Framework) {
		fs = append(fs, e.buildCatalogFacet(dataset.Catalog, base.AtName("catalog")))
	}
	if e.cap.isDatasetEnabled(FacetDatasetTags) && len(dataset.Tags) > 0 {
		fs = append(fs, e.buildTagsDatasetFacet(dataset.Tags, base.AtName("tags")))
	}

	return fs
}

// ── Individual dataset facet builders ────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildSymlinksFacet(symlinks []SymlinksDatasetModel, base path.Path) *facets.SymlinksDatasetFacet {
	identifiers := make([]facets.Identifier, 0, len(symlinks))
	for i, s := range symlinks {
		p := base.AtListIndex(i)
		identifiers = append(identifiers, facets.Identifier{
			Name:      e.requireString(s.Name, p.AtName("name")),
			Namespace: e.requireString(s.Namespace, p.AtName("namespace")),
			Type:      e.requireString(s.Type, p.AtName("type")),
		})
	}
	return facets.NewSymlinksDatasetFacet(producer).WithIdentifiers(identifiers)
}

func (e *OpenLineageEventBuilder) buildSchemaFacet(s *SchemaDatasetModel, base path.Path) *facets.SchemaDatasetFacet {
	fields := make([]facets.FieldElement, 0, len(s.Fields))
	for i, f := range s.Fields {
		p := base.AtName("fields").AtListIndex(i)
		fe := facets.FieldElement{Name: e.requireString(f.Name, p.AtName("name"))}
		fe.Type = stringPtrIfKnown(f.Type)
		fe.Description = stringPtrIfKnown(f.Description)
		fields = append(fields, fe)
	}
	return facets.NewSchemaDatasetFacet(producer).WithFields(fields)
}

func (e *OpenLineageEventBuilder) buildDocumentationDatasetFacet(data *DocumentationModel, base path.Path) *facets.DocumentationDatasetFacet {
	df := facets.NewDocumentationDatasetFacet(producer, e.requireString(data.Description, base.AtName("description")))
	df.ContentType = stringPtrIfKnown(data.ContentType)
	return df
}

func (e *OpenLineageEventBuilder) buildDataSourceFacet(ds *DataSourceDatasetModel, base path.Path) *facets.DatasourceDatasetFacet {
	f := facets.NewDatasourceDatasetFacet(producer)
	f = f.WithName(e.requireString(ds.Name, base.AtName("name")))
	f = f.WithURI(e.requireString(ds.URI, base.AtName("uri")))
	return f
}

func (e *OpenLineageEventBuilder) buildDatasetTypeFacet(dt *DatasetTypeDatasetModel, base path.Path) *facets.DatasetTypeDatasetFacet {
	f := facets.NewDatasetTypeDatasetFacet(producer, e.requireString(dt.DatasetType, base.AtName("dataset_type")))
	if isKnownString(dt.SubType) {
		f = f.WithSubType(dt.SubType.ValueString())
	}
	return f
}

func (e *OpenLineageEventBuilder) buildDatasetVersionFacet(data *DatasetVersionDatasetModel, base path.Path) *facets.DatasetVersionDatasetFacet {
	return facets.NewDatasetVersionDatasetFacet(producer,
		e.requireString(data.DatasetVersion, base.AtName("dataset_version")))
}

func (e *OpenLineageEventBuilder) buildStorageFacet(s *StorageDatasetModel, base path.Path) *facets.StorageDatasetFacet {
	f := facets.NewStorageDatasetFacet(producer, e.requireString(s.StorageLayer, base.AtName("storage_layer")))
	if isKnownString(s.FileFormat) {
		f = f.WithFileFormat(s.FileFormat.ValueString())
	}
	return f
}

func (e *OpenLineageEventBuilder) buildOwnershipDatasetFacet(o *OwnershipDatasetModel, base path.Path) *facets.OwnershipDatasetFacet {
	owners := make([]facets.Owner, 0, len(o.Owners))
	for i, owner := range o.Owners {
		owners = append(owners, e.buildOwner(owner.Name, owner.Type, base.AtName("owners").AtListIndex(i)))
	}
	return facets.NewOwnershipDatasetFacet(producer).WithOwners(owners)
}

func (e *OpenLineageEventBuilder) buildOwner(name types.String, ownerType types.String, base path.Path) facets.Owner {
	owner := facets.Owner{Name: e.requireString(name, base.AtName("name"))}
	owner.Type = stringPtrIfKnown(ownerType)
	return owner
}

func (e *OpenLineageEventBuilder) buildLifecycleStateChangeFacet(lsc *LifecycleStateChangeDatasetModel, base path.Path) *facets.LifecycleStateChangeDatasetFacet {
	f := facets.NewLifecycleStateChangeDatasetFacet(
		producer,
		facets.LifecycleStateChangeEnum(e.requireString(lsc.LifecycleStateChange, base.AtName("lifecycle_state_change"))),
	)
	if lsc.PreviousIdentifier != nil {
		p := base.AtName("previous_identifier")
		f = f.WithPreviousIdentifier(&facets.PreviousIdentifier{
			Name:      e.requireString(lsc.PreviousIdentifier.Name, p.AtName("name")),
			Namespace: e.requireString(lsc.PreviousIdentifier.Namespace, p.AtName("namespace")),
		})
	}
	return f
}

func (e *OpenLineageEventBuilder) buildHierarchyFacet(h *HierarchyDatasetModel, base path.Path) *facets.HierarchyDatasetFacet {
	elements := make([]facets.HierarchyElement, 0, len(h.Hierarchy))
	for i, level := range h.Hierarchy {
		p := base.AtName("hierarchy").AtListIndex(i)
		elements = append(elements, facets.HierarchyElement{
			Name: e.requireString(level.Name, p.AtName("name")),
			Type: e.requireString(level.Type, p.AtName("type")),
		})
	}
	return facets.NewHierarchyDatasetFacet(producer).WithHierarchy(elements)
}

func (e *OpenLineageEventBuilder) buildTagsDatasetFacet(tags []TagsDatasetModel, base path.Path) *facets.TagsDatasetFacet {
	elements := make([]facets.TagElement, 0, len(tags))
	for i, t := range tags {
		p := base.AtListIndex(i)
		tagElement := facets.TagElement{
			Key:   e.requireString(t.Name, p.AtName("name")),
			Value: e.requireString(t.Value, p.AtName("value")),
		}
		tagElement.Source = stringPtrIfKnown(t.Source)
		tagElement.Field = stringPtrIfKnown(t.Field)
		elements = append(elements, tagElement)
	}
	return facets.NewTagsDatasetFacet(producer).WithTags(elements)
}

func (e *OpenLineageEventBuilder) buildCatalogFacet(c *CatalogDatasetModel, base path.Path) *facets.CatalogDatasetFacet {
	cat := facets.NewCatalogDatasetFacet(
		producer,
		e.requireString(c.Framework, base.AtName("framework")),
		e.requireString(c.Type, base.AtName("type")),
		e.requireString(c.Name, base.AtName("name")),
	)
	cat.MetadataURI = stringPtrIfKnown(c.MetadataURI)
	cat.WarehouseURI = stringPtrIfKnown(c.WarehouseURI)
	cat.Source = stringPtrIfKnown(c.Source)

	if !c.CatalogProperties.IsNull() && !c.CatalogProperties.IsUnknown() && len(c.CatalogProperties.Elements()) > 0 {
		props := make(map[string]string, len(c.CatalogProperties.Elements()))
		for k, v := range c.CatalogProperties.Elements() {
			if sv, ok := v.(types.String); ok && !sv.IsNull() && !sv.IsUnknown() {
				props[k] = sv.ValueString()
			}
		}
		if len(props) > 0 {
			cat = cat.WithCatalogProperties(props)
		}
	}
	return cat
}

// ── Column lineage ────────────────────────────────────────────────────────────

// buildColumnLineageFacet creates a ColumnLineageFacet from the Terraform column_lineage blocks.
//
// The OL ColumnLineageFacet has two parts:
//  1. Fields map[string]FieldValue — keyed by output column name.
//  2. Dataset []DatasetElement — dataset-level lineage (input dataset → output field).
func (e *OpenLineageEventBuilder) buildColumnLineageFacet(clm *ColumnLineageDatasetModel, base path.Path) *facets.ColumnLineageDatasetFacet {
	fields := make(map[string]facets.FieldValue)
	var datasetElements []facets.DatasetElement

	fieldsBase := base.AtName("fields")
	for fi, f := range clm.Fields {
		fp := fieldsBase.AtListIndex(fi)
		var inputFields []facets.DatasetElement
		for ii, inf := range f.InputFields {
			ip := fp.AtName("input_fields").AtListIndex(ii)
			inputFields = append(inputFields, e.buildDatasetElement(inf.Name, inf.Namespace, inf.Field, inf.Transformations, ip))
		}

		fieldName := e.requireString(f.Name, fp.AtName("name"))
		fv := facets.FieldValue{InputFields: inputFields}
		if existing, ok := fields[fieldName]; ok {
			existing.InputFields = append(existing.InputFields, inputFields...)
			fields[fieldName] = existing
		} else {
			fields[fieldName] = fv
		}
	}

	datasetBase := base.AtName("dataset")
	for di, ds := range clm.Dataset {
		dp := datasetBase.AtListIndex(di)
		datasetElements = append(datasetElements, e.buildDatasetElement(ds.Name, ds.Namespace, ds.Field, ds.Transformations, dp))
	}

	cl := facets.NewColumnLineageDatasetFacet(producer).WithFields(fields)
	if len(datasetElements) > 0 {
		cl = cl.WithDataset(datasetElements)
	}
	return cl
}

func (e *OpenLineageEventBuilder) buildDatasetElement(name types.String, namespace types.String, field types.String, transformations []TransformationModel, base path.Path) facets.DatasetElement {
	de := facets.DatasetElement{
		Name:      e.requireString(name, base.AtName("name")),
		Namespace: e.requireString(namespace, base.AtName("namespace")),
		Field:     e.requireString(field, base.AtName("field")),
	}
	for i, t := range transformations {
		tr := t
		de.Transformations = append(de.Transformations, e.buildTransformation(&tr, base.AtName("transformations").AtListIndex(i)))
	}
	return de
}

func (e *OpenLineageEventBuilder) buildTransformation(t *TransformationModel, base path.Path) facets.Transformation {
	tr := facets.Transformation{
		Type: e.requireString(t.Type, base.AtName("type")),
	}
	tr.Subtype = stringPtrIfKnown(t.Subtype)
	tr.Description = stringPtrIfKnown(t.Description)
	tr.Masking = boolPtrIfKnown(t.Masking)
	return tr
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// requireString returns the string value of attr. If it is null or unknown,
// an attribute error is added to Diagnostics with the supplied path.
func (e *OpenLineageEventBuilder) requireString(attr types.String, p path.Path) string {
	if attr.IsNull() || attr.IsUnknown() {
		e.Diagnostics.AddAttributeError(p, "Missing required attribute",
			fmt.Sprintf("Attribute %q must be set. The attribute is validated in config so it shouldn't happen", p))
	}
	return attr.ValueString()
}

func isKnownString(v types.String) bool {
	return !v.IsNull() && !v.IsUnknown()
}

func stringPtrIfKnown(v types.String) *string {
	if !isKnownString(v) {
		return nil
	}
	return openlineage.Ptr(v.ValueString())
}

func boolPtrIfKnown(v types.Bool) *bool {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	return openlineage.Ptr(v.ValueBool())
}
