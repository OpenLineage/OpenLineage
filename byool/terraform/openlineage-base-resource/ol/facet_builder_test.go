/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"testing"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// ── job facet: job_type ───────────────────────────────────────────────────────

func TestBuildRunEvent_JobType_IncludedWhenFacetEnabled(t *testing.T) {
	model := minimalModel()
	model.JobType = jobTypeModel(true, false)

	jt := jobFacets(BuildRunEvent(model, fullJobCapability())).JobTypeJobFacet

	if jt == nil {
		t.Fatal("expected jobType facet to be present")
	}
	if jt.ProcessingType != "test" {
		t.Errorf("expected ProcessingType = %q, got %q", "test", jt.ProcessingType)
	}
	if jt.Integration != "test" {
		t.Errorf("expected Integration = %q, got %q", "test", jt.Integration)
	}
}

func TestBuildRunEvent_JobType_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	model := minimalModel()
	model.JobType = jobTypeModel(false, false)

	var diags diag.Diagnostics
	NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(model)

	if !diags.HasError() {
		t.Error("expected a diagnostic error when required fields are null")
	}
}

func TestBuildRunEvent_JobType_OptionalJobTypeField(t *testing.T) {
	model := minimalModel()
	model.JobType = jobTypeModel(true, true)

	jt := jobFacets(BuildRunEvent(model, fullJobCapability())).JobTypeJobFacet

	if jt == nil {
		t.Fatal("expected jobType facet to be present")
	}
	if jt.JobType == nil || *jt.JobType != "test" {
		t.Errorf("expected optional JobType = %q, got %v", "test", jt.JobType)
	}
}

// ── job facet: ownership ──────────────────────────────────────────────────────

func TestBuildRunEvent_Ownership_OmittedWhenDisabled(t *testing.T) {
	model := minimalModel()
	model.Ownership = ownershipJobModel(1, true, false)

	if jobFacets(BuildRunEvent(model, EmptyJobCapability())).OwnershipJobFacet != nil {
		t.Error("expected ownership facet to be absent when FacetJobOwnership is disabled")
	}
}

func TestBuildRunEvent_Ownership_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.Ownership = ownershipJobModel(1, true, false)

	ow := jobFacets(BuildRunEvent(model, fullJobCapability())).OwnershipJobFacet

	if ow == nil {
		t.Fatal("expected ownership facet to be present")
	}
	if len(ow.Owners) != 1 || ow.Owners[0].Name != "test" {
		t.Errorf("expected 1 owner with name %q, got %v", "test", ow.Owners)
	}
}

// ── job facet: documentation ──────────────────────────────────────────────────

func TestBuildRunEvent_Documentation_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.Documentation = documentationModel(true, true)

	doc := jobFacets(BuildRunEvent(model, fullJobCapability())).DocumentationJobFacet

	if doc == nil {
		t.Fatal("expected documentation facet to be present")
	}
	if doc.Description != "test" {
		t.Errorf("expected Description = %q, got %q", "test", doc.Description)
	}
	if doc.ContentType == nil || *doc.ContentType != "test" {
		t.Errorf("expected ContentType = %q, got %v", "test", doc.ContentType)
	}
}

func TestBuildRunEvent_Documentation_SkippedWhenDescriptionIsNull(t *testing.T) {
	model := minimalModel()
	model.Documentation = documentationModel(false, false)

	var diags diag.Diagnostics
	NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(model)

	if !diags.HasError() {
		t.Error("expected a diagnostic error when Description is null")
	}
}

// ── job facet: source_code ────────────────────────────────────────────────────

func TestBuildRunEvent_SourceCode_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SourceCode = sourceCodeModel(true)

	sc := jobFacets(BuildRunEvent(model, fullJobCapability())).SourceCodeJobFacet

	if sc == nil {
		t.Fatal("expected sourceCode facet to be present")
	}
	if sc.Language != "test" {
		t.Errorf("expected Language = %q, got %q", "test", sc.Language)
	}
}

// ── job facet: sql ────────────────────────────────────────────────────────────

func TestBuildRunEvent_SQL_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SQL = sqlModel(true, false)

	sf := jobFacets(BuildRunEvent(model, fullJobCapability())).SQLJobFacet

	if sf == nil {
		t.Fatal("expected sql facet to be present")
	}
	if sf.Query != "test" {
		t.Errorf("expected Query = %q, got %q", "test", sf.Query)
	}
}

// ── job facet: tags ───────────────────────────────────────────────────────────

func TestBuildRunEvent_Tags_SkipsNullEntries(t *testing.T) {
	model := minimalModel()
	model.Tags = jobTags("env", "prod", "", "orphaned") // "" → null name → skip

	tf := jobFacets(BuildRunEvent(model, fullJobCapability())).TagsJobFacet

	if tf == nil {
		t.Fatal("expected tags facet to be present")
	}
	if len(tf.Tags) != 1 {
		t.Fatalf("expected 1 tag (null entry skipped), got %d", len(tf.Tags))
	}
	if tf.Tags[0].Key != "env" {
		t.Errorf("expected tag key = %q, got %q", "env", tf.Tags[0].Key)
	}
}

func TestBuildRunEvent_Tags_OmittedWhenAllEntriesAreNull(t *testing.T) {
	model := minimalModel()
	model.Tags = jobTags("", "") // both null → facet must be absent

	if jobFacets(BuildRunEvent(model, fullJobCapability())).TagsJobFacet != nil {
		t.Error("expected tags facet to be absent when all entries have null name/value")
	}
}

// ── job facet: source_code_location ──────────────────────────────────────────

func TestBuildRunEvent_SourceCodeLocation_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SourceCodeLocation = sourceCodeLocationModel(true, true)

	scl := jobFacets(BuildRunEvent(model, fullJobCapability())).SourceCodeLocationJobFacet

	if scl == nil {
		t.Fatal("expected sourceCodeLocation facet to be present")
	}
	if scl.Type != "test" {
		t.Errorf("expected Type = %q, got %q", "test", scl.Type)
	}
	if scl.Branch == nil || *scl.Branch != "test" {
		t.Errorf("expected Branch = %q, got %v", "test", scl.Branch)
	}
}

func TestBuildRunEvent_SourceCodeLocation_SkippedWhenTypeIsNull(t *testing.T) {
	model := minimalModel()
	model.SourceCodeLocation = sourceCodeLocationModel(false, false)

	var diags diag.Diagnostics
	NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(model)

	if !diags.HasError() {
		t.Error("expected a diagnostic error when Type is null")
	}
}

// ── dataset facet: symlinks ───────────────────────────────────────────────────

func TestBuildRunEvent_Symlinks_OmittedOnInputWhenDisabled(t *testing.T) {
	model := simpleJobModel()
	ds := datasetModel("hive", "db.table")
	ds.Symlinks = symlinksModel(1, true)
	model.Inputs = olInputs(ds)

	if inputFacets(BuildRunEvent(model, EmptyJobCapability()), 0).SymlinksDatasetFacet != nil {
		t.Error("expected symlinks facet to be absent when FacetDatasetSymlinks is disabled")
	}
}

func TestBuildRunEvent_Symlinks_IncludedOnInputWhenEnabled(t *testing.T) {
	model := simpleJobModel()
	ds := datasetModel("hive", "db.table")
	ds.Symlinks = symlinksModel(1, true)
	model.Inputs = olInputs(ds)

	sl := inputFacets(BuildRunEvent(model, fullJobCapability()), 0).SymlinksDatasetFacet

	if sl == nil {
		t.Fatal("expected symlinks facet to be present")
	}
	if len(sl.Identifiers) != 1 || sl.Identifiers[0].Namespace != "test" {
		t.Errorf("expected 1 symlink with namespace = %q, got %v", "test", sl.Identifiers)
	}
}

// ── dataset facet: column_lineage ─────────────────────────────────────────────

func TestBuildRunEvent_ColumnLineage_OmittedOnOutputWhenDisabled(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{columnLineageOutput("bq", "bq.out", "col", "bq", "bq.in", "src_col")}

	if outputFacets(BuildRunEvent(model, EmptyJobCapability()), 0).ColumnLineageDatasetFacet != nil {
		t.Error("expected columnLineage to be absent when FacetDatasetColumnLineage is disabled")
	}
}

func TestBuildRunEvent_ColumnLineage_IncludedOnOutputWhenEnabled(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{columnLineageOutput("bq", "bq.output", "output_col", "bq", "bq.input", "input_col")}

	cl := outputFacets(BuildRunEvent(model, fullJobCapability()), 0).ColumnLineageDatasetFacet

	if cl == nil {
		t.Fatal("expected columnLineage facet to be present")
	}
	fv, ok := cl.Fields["output_col"]
	if !ok {
		t.Fatal("expected 'output_col' in column lineage fields")
	}
	if len(fv.InputFields) != 1 || fv.InputFields[0].Field != "input_col" {
		t.Errorf("expected input field %q, got %v", "input_col", fv.InputFields)
	}
}

func TestBuildRunEvent_ColumnLineage_NotEmittedOnInputs(t *testing.T) {
	model := simpleJobModel()
	model.Inputs = olInputs(datasetModel("bq", "bq.in"))

	event := BuildRunEvent(model, fullJobCapability())

	if len(event.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(event.Inputs))
	}
	// column_lineage is output-only — inputs have no ColumnLineageDatasetFacet
	if inputFacets(event, 0).ColumnLineageDatasetFacet != nil {
		t.Error("expected columnLineage to be absent on inputs")
	}
}

func TestBuildRunEvent_ColumnLineage_DuplicateFieldNamesAreAggregated(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{
		{
			DatasetModel: datasetModel("bq", "bq.out"),
			ColumnLineage: &ColumnLineageDatasetModel{
				Fields: []ColumnLineageFieldModel{
					{
						Name:        types.StringValue("out_col"),
						InputFields: []InputFieldModel{{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("col_a")}},
					},
					{
						Name:        types.StringValue("out_col"), // same output column — must merge
						InputFields: []InputFieldModel{{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("col_b")}},
					},
				},
			},
		},
	}

	cl := outputFacets(BuildRunEvent(model, fullJobCapability()), 0).ColumnLineageDatasetFacet

	if cl == nil {
		t.Fatal("expected columnLineage facet to be present")
	}
	fv := cl.Fields["out_col"]
	if len(fv.InputFields) != 2 {
		t.Fatalf("expected 2 merged input fields, got %d", len(fv.InputFields))
	}
	got := map[string]bool{fv.InputFields[0].Field: true, fv.InputFields[1].Field: true}
	if !got["col_a"] {
		t.Error("expected col_a after aggregation")
	}
	if !got["col_b"] {
		t.Error("expected col_b after aggregation")
	}
}

// ── dataset facet: column_lineage transformations ─────────────────────────────

func getTransformation(t *testing.T, event *openlineage.RunEvent, outputIdx int, fieldName string) facets.Transformation {
	t.Helper()
	cl := outputFacets(event, outputIdx).ColumnLineageDatasetFacet
	if cl == nil {
		t.Fatal("expected columnLineage facet to be present")
	}
	fv, ok := cl.Fields[fieldName]
	if !ok || len(fv.InputFields) == 0 {
		t.Fatalf("expected input fields for %q", fieldName)
	}
	if len(fv.InputFields[0].Transformations) == 0 {
		t.Fatal("expected at least one transformation")
	}
	return fv.InputFields[0].Transformations[0]
}

func TestBuildRunEvent_Transformation_DirectType(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{columnLineageOutput("bq", "bq.out", "out_col", "bq", "bq.in", "in_col",
		transformationModel(true, false))}

	tr := getTransformation(t, BuildRunEvent(model, fullJobCapability()), 0, "out_col")

	if tr.Type != "test" {
		t.Errorf("expected Type = %q, got %q", "test", tr.Type)
	}
	if tr.Subtype != nil {
		t.Errorf("expected Subtype nil when optional=false, got %q", *tr.Subtype)
	}
}

func TestBuildRunEvent_Transformation_OptionalFieldsIncluded(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{columnLineageOutput("bq", "bq.out", "masked_col", "bq", "bq.in", "pii_col",
		transformationModel(true, true))}

	tr := getTransformation(t, BuildRunEvent(model, fullJobCapability()), 0, "masked_col")

	if tr.Masking == nil || !*tr.Masking {
		t.Errorf("expected Masking = true, got %v", tr.Masking)
	}
	if tr.Subtype == nil || *tr.Subtype != "test" {
		t.Errorf("expected Subtype = %q, got %v", "test", tr.Subtype)
	}
}

func TestBuildRunEvent_Transformation_NullOptionalFieldsOmitted(t *testing.T) {
	model := simpleJobModel()
	model.Outputs = []OLOutputModel{columnLineageOutput("bq", "bq.out", "col", "bq", "bq.in", "src",
		TransformationModel{Type: types.StringValue("DIRECT"), Subtype: types.StringNull(), Description: types.StringNull(), Masking: types.BoolNull()})}

	tr := getTransformation(t, BuildRunEvent(model, fullJobCapability()), 0, "col")

	if tr.Subtype != nil {
		t.Errorf("expected Subtype to be nil, got %q", *tr.Subtype)
	}
	if tr.Description != nil {
		t.Errorf("expected Description to be nil, got %q", *tr.Description)
	}
	if tr.Masking != nil {
		t.Errorf("expected Masking to be nil, got %v", *tr.Masking)
	}
}

// ── dataset facet: storage (required-field diagnostics) ───────────────────────

func TestBuildRunEvent_Storage_SkippedWhenStorageLayerIsNull(t *testing.T) {
	model := simpleJobModel()
	ds := datasetModel("bq", "bq.in")
	ds.Storage = storageModel(false, true) // required=false → StorageLayer null → diagnostic error
	model.Inputs = olInputs(ds)

	var diags diag.Diagnostics
	NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(model)

	if !diags.HasError() {
		t.Error("expected a diagnostic error when StorageLayer is null")
	}
}

// ── dataset facet: catalog (required-field diagnostics) ───────────────────────

func TestBuildRunEvent_Catalog_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	model := simpleJobModel()
	ds := datasetModel("bq", "bq.in")
	ds.Catalog = catalogModel(false, false) // required=false → Framework/Type/Name null → diagnostic error
	model.Inputs = olInputs(ds)

	var diags diag.Diagnostics
	NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(model)

	if !diags.HasError() {
		t.Error("expected a diagnostic error when required catalog fields are null")
	}
}
