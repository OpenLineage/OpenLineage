/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// ── Interfaces ────────────────────────────────────────────────────────────────

// JobFacetBuilder is implemented by job facet models that can produce an OL job facet.
// Returning nil from BuildJobFacet is valid and signals that the facet should be skipped
// (e.g. when no valid tag entries are present).
type JobFacetBuilder interface {
	BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet
}

// DatasetFacetBuilder is implemented by dataset facet models that can produce an OL dataset facet.
type DatasetFacetBuilder interface {
	BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet
}

// ── Job facet models ──────────────────────────────────────────────────────────

func (m *JobTypeJobModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	jt := facets.NewJobTypeJobFacet(
		producer,
		requireString(diags, m.Integration, base.AtName("integration")),
		requireString(diags, m.ProcessingType, base.AtName("processing_type")),
	)
	jt.JobType = stringPtrIfKnown(m.JobType)
	return jt
}

func (m *OwnershipJobModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	owners := make([]facets.Owner, 0, len(m.Owners))
	for i, o := range m.Owners {
		owners = append(owners, buildOwner(diags, o.Name, o.Type, base.AtName("owners").AtListIndex(i)))
	}
	return facets.NewOwnershipJobFacet(producer).WithOwners(owners)
}

func (m *DocumentationModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	df := facets.NewDocumentationJobFacet(producer, requireString(diags, m.Description, base.AtName("description")))
	df.ContentType = stringPtrIfKnown(m.ContentType)
	return df
}

func (m *SourceCodeJobModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	return facets.NewSourceCodeJobFacet(
		producer,
		requireString(diags, m.Language, base.AtName("language")),
		requireString(diags, m.SourceCode, base.AtName("source_code")),
	)
}

func (m *SourceCodeLocationJobModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	scl := facets.NewSourceCodeLocationJobFacet(
		producer,
		requireString(diags, m.Type, base.AtName("type")),
		requireString(diags, m.URL, base.AtName("url")),
	)
	scl.RepoURL = stringPtrIfKnown(m.RepoURL)
	scl.Path = stringPtrIfKnown(m.Path)
	scl.Version = stringPtrIfKnown(m.Version)
	scl.Tag = stringPtrIfKnown(m.Tag)
	scl.Branch = stringPtrIfKnown(m.Branch)
	return scl
}

func (m *SQLJobModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	sf := facets.NewSQLJobFacet(producer, requireString(diags, m.Query, base.AtName("query")))
	if isKnownString(m.Dialect) {
		sf = sf.WithDialect(m.Dialect.ValueString())
	}
	return sf
}

func (m *TagsJobFacetModel) BuildJobFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.JobFacet {
	tags := make([]facets.TagClass, 0, len(m.Tag))
	for i, t := range m.Tag {
		tp := base.AtName("tag").AtListIndex(i)
		if !isKnownString(t.Name) || !isKnownString(t.Value) {
			_ = requireString(diags, t.Name, tp.AtName("name"))
			_ = requireString(diags, t.Value, tp.AtName("value"))
			continue
		}
		tc := facets.TagClass{
			Key:   requireString(diags, t.Name, tp.AtName("name")),
			Value: requireString(diags, t.Value, tp.AtName("value")),
		}
		tc.Source = stringPtrIfKnown(t.Source)
		tags = append(tags, tc)
	}
	if len(tags) == 0 {
		return nil
	}
	return facets.NewTagsJobFacet(producer).WithTags(tags)
}

// ── Dataset facet models ──────────────────────────────────────────────────────

func (m *DocumentationModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	df := facets.NewDocumentationDatasetFacet(producer, requireString(diags, m.Description, base.AtName("description")))
	df.ContentType = stringPtrIfKnown(m.ContentType)
	return df
}

func (m *SymlinksDatasetFacetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	identifiers := make([]facets.Identifier, 0, len(m.Identifier))
	for i, s := range m.Identifier {
		p := base.AtName("symlink").AtListIndex(i)
		identifiers = append(identifiers, facets.Identifier{
			Name:      requireString(diags, s.Name, p.AtName("name")),
			Namespace: requireString(diags, s.Namespace, p.AtName("namespace")),
			Type:      requireString(diags, s.Type, p.AtName("type")),
		})
	}
	return facets.NewSymlinksDatasetFacet(producer).WithIdentifiers(identifiers)
}

func (m *SchemaDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	fields := make([]facets.FieldElement, 0, len(m.Fields))
	for i, f := range m.Fields {
		p := base.AtName("fields").AtListIndex(i)
		fe := facets.FieldElement{Name: requireString(diags, f.Name, p.AtName("name"))}
		fe.Type = stringPtrIfKnown(f.Type)
		fe.Description = stringPtrIfKnown(f.Description)
		fields = append(fields, fe)
	}
	return facets.NewSchemaDatasetFacet(producer).WithFields(fields)
}

func (m *DataSourceDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	f := facets.NewDatasourceDatasetFacet(producer)
	f = f.WithName(requireString(diags, m.Name, base.AtName("name")))
	f = f.WithURI(requireString(diags, m.URI, base.AtName("uri")))
	return f
}

func (m *DatasetTypeDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	f := facets.NewDatasetTypeDatasetFacet(producer, requireString(diags, m.DatasetType, base.AtName("dataset_type")))
	if isKnownString(m.SubType) {
		f = f.WithSubType(m.SubType.ValueString())
	}
	return f
}

func (m *DatasetVersionDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	return facets.NewDatasetVersionDatasetFacet(producer,
		requireString(diags, m.DatasetVersion, base.AtName("dataset_version")))
}

func (m *StorageDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	f := facets.NewStorageDatasetFacet(producer, requireString(diags, m.StorageLayer, base.AtName("storage_layer")))
	if isKnownString(m.FileFormat) {
		f = f.WithFileFormat(m.FileFormat.ValueString())
	}
	return f
}

func (m *OwnershipDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	owners := make([]facets.Owner, 0, len(m.Owners))
	for i, o := range m.Owners {
		owners = append(owners, buildOwner(diags, o.Name, o.Type, base.AtName("owners").AtListIndex(i)))
	}
	return facets.NewOwnershipDatasetFacet(producer).WithOwners(owners)
}

func (m *LifecycleStateChangeDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	f := facets.NewLifecycleStateChangeDatasetFacet(
		producer,
		facets.LifecycleStateChangeEnum(requireString(diags, m.LifecycleStateChange, base.AtName("lifecycle_state_change"))),
	)
	if m.PreviousIdentifier != nil {
		p := base.AtName("previous_identifier")
		f = f.WithPreviousIdentifier(&facets.PreviousIdentifier{
			Name:      requireString(diags, m.PreviousIdentifier.Name, p.AtName("name")),
			Namespace: requireString(diags, m.PreviousIdentifier.Namespace, p.AtName("namespace")),
		})
	}
	return f
}

func (m *HierarchyDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	elements := make([]facets.HierarchyElement, 0, len(m.Hierarchy))
	for i, level := range m.Hierarchy {
		p := base.AtName("hierarchy").AtListIndex(i)
		elements = append(elements, facets.HierarchyElement{
			Name: requireString(diags, level.Name, p.AtName("name")),
			Type: requireString(diags, level.Type, p.AtName("type")),
		})
	}
	return facets.NewHierarchyDatasetFacet(producer).WithHierarchy(elements)
}

func (m *CatalogDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	cat := facets.NewCatalogDatasetFacet(
		producer,
		requireString(diags, m.Framework, base.AtName("framework")),
		requireString(diags, m.Type, base.AtName("type")),
		requireString(diags, m.Name, base.AtName("name")),
	)
	cat.MetadataURI = stringPtrIfKnown(m.MetadataURI)
	cat.WarehouseURI = stringPtrIfKnown(m.WarehouseURI)
	cat.Source = stringPtrIfKnown(m.Source)
	if !m.CatalogProperties.IsNull() && !m.CatalogProperties.IsUnknown() && len(m.CatalogProperties.Elements()) > 0 {
		props := make(map[string]string, len(m.CatalogProperties.Elements()))
		for k, v := range m.CatalogProperties.Elements() {
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

func (m *TagsDatasetFacetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	elements := make([]facets.TagElement, 0, len(m.Tag))
	for i, t := range m.Tag {
		p := base.AtName("tag").AtListIndex(i)
		tagElement := facets.TagElement{
			Key:   requireString(diags, t.Name, p.AtName("name")),
			Value: requireString(diags, t.Value, p.AtName("value")),
		}
		tagElement.Source = stringPtrIfKnown(t.Source)
		tagElement.Field = stringPtrIfKnown(t.Field)
		elements = append(elements, tagElement)
	}
	return facets.NewTagsDatasetFacet(producer).WithTags(elements)
}

func (m *ColumnLineageDatasetModel) BuildDatasetFacet(producer string, diags *diag.Diagnostics, base path.Path) facets.DatasetFacet {
	fields := make(map[string]facets.FieldValue)
	var datasetElements []facets.DatasetElement

	fieldsBase := base.AtName("fields")
	for fi, f := range m.Fields {
		fp := fieldsBase.AtListIndex(fi)
		var inputFields []facets.DatasetElement
		for ii, inf := range f.InputFields {
			ip := fp.AtName("input_fields").AtListIndex(ii)
			inputFields = append(inputFields, buildDatasetElement(diags, inf.Name, inf.Namespace, inf.Field, inf.Transformations, ip))
		}
		fieldName := requireString(diags, f.Name, fp.AtName("name"))
		fv := facets.FieldValue{InputFields: inputFields}
		if existing, ok := fields[fieldName]; ok {
			existing.InputFields = append(existing.InputFields, inputFields...)
			fields[fieldName] = existing
		} else {
			fields[fieldName] = fv
		}
	}

	datasetBase := base.AtName("dataset")
	for di, ds := range m.Dataset {
		dp := datasetBase.AtListIndex(di)
		datasetElements = append(datasetElements, buildDatasetElement(diags, ds.Name, ds.Namespace, ds.Field, ds.Transformations, dp))
	}

	cl := facets.NewColumnLineageDatasetFacet(producer).WithFields(fields)
	if len(datasetElements) > 0 {
		cl = cl.WithDataset(datasetElements)
	}
	return cl
}

// ── Shared sub-element helpers ────────────────────────────────────────────────

func buildOwner(diags *diag.Diagnostics, name types.String, ownerType types.String, base path.Path) facets.Owner {
	o := facets.Owner{Name: requireString(diags, name, base.AtName("name"))}
	o.Type = stringPtrIfKnown(ownerType)
	return o
}

func buildDatasetElement(diags *diag.Diagnostics, name, namespace, field types.String, transformations []TransformationModel, base path.Path) facets.DatasetElement {
	de := facets.DatasetElement{
		Name:      requireString(diags, name, base.AtName("name")),
		Namespace: requireString(diags, namespace, base.AtName("namespace")),
		Field:     requireString(diags, field, base.AtName("field")),
	}
	for i, t := range transformations {
		tr := t
		de.Transformations = append(de.Transformations, buildTransformation(diags, &tr, base.AtName("transformations").AtListIndex(i)))
	}
	return de
}

func buildTransformation(diags *diag.Diagnostics, t *TransformationModel, base path.Path) facets.Transformation {
	tr := facets.Transformation{
		Type: requireString(diags, t.Type, base.AtName("type")),
	}
	tr.Subtype = stringPtrIfKnown(t.Subtype)
	tr.Description = stringPtrIfKnown(t.Description)
	tr.Masking = boolPtrIfKnown(t.Masking)
	return tr
}
