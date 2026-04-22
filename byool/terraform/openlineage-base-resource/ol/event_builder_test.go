/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"testing"

	"github.com/OpenLineage/openlineage/client/go/pkg/facets"
	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// minimalModel returns a JobResourceModel with only the required fields set.
func minimalModel() *JobResourceModel {
	return &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("test-namespace"),
			Name:      types.StringValue("test-job"),
		},
	}
}

// jobFacets returns event.Job.Facets, or an empty JobFacets if nil — lets tests
// access fields without nil-checking every time.
func jobFacets(event *openlineage.RunEvent) *facets.JobFacets {
	if event.Job.Facets == nil {
		return &facets.JobFacets{}
	}
	return event.Job.Facets
}

// inputFacets returns event.Inputs[i].Facets, or an empty DatasetFacets if nil.
func inputFacets(event *openlineage.RunEvent, i int) *facets.DatasetFacets {
	if event.Inputs[i].Facets == nil {
		return &facets.DatasetFacets{}
	}
	return event.Inputs[i].Facets
}

// outputFacets returns event.Outputs[i].Facets, or an empty DatasetFacets if nil.
func outputFacets(event *openlineage.RunEvent, i int) *facets.DatasetFacets {
	if event.Outputs[i].Facets == nil {
		return &facets.DatasetFacets{}
	}
	return event.Outputs[i].Facets
}

// ── BuildRunEvent — basic fields ──────────────────────────────────────────────

func TestBuildRunEvent_ReturnsNonNil(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())
	if event == nil {
		t.Fatal("BuildRunEvent returned nil")
	}
}

func TestBuildRunEvent_SetsJobNameAndNamespace(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())

	if event.Job.Name != "test-job" {
		t.Errorf("expected Job.Name = %q, got %q", "test-job", event.Job.Name)
	}
	if event.Job.Namespace != "test-namespace" {
		t.Errorf("expected Job.Namespace = %q, got %q", "test-namespace", event.Job.Namespace)
	}
}

func TestBuildRunEvent_GeneratesNonEmptyRunID(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())

	if event.Run.RunID == "" {
		t.Error("expected Run.RunID to be a non-empty UUID")
	}
}

func TestBuildRunEvent_GeneratesUniqueRunIDs(t *testing.T) {
	model := minimalModel()
	e1 := BuildRunEvent(model, EmptyJobCapability())
	e2 := BuildRunEvent(model, EmptyJobCapability())

	if e1.Run.RunID == e2.Run.RunID {
		t.Errorf("expected unique RunIDs per call, got same value: %q", e1.Run.RunID)
	}
}

func TestBuildRunEvent_SetsSchemaURL(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())

	if event.SchemaURL == "" {
		t.Error("expected SchemaURL to be set")
	}
	if event.SchemaURL != openlineage.RunEventSchemaURL {
		t.Errorf("expected SchemaURL = %q, got %q", openlineage.RunEventSchemaURL, event.SchemaURL)
	}
}

func TestBuildRunEvent_EventTypeIsComplete(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())

	if event.EventType != openlineage.EventTypeComplete {
		t.Errorf("expected EventType = %q, got %q", openlineage.EventTypeComplete, event.EventType)
	}
}

// ── BuildRunEvent — job facet filtering ──────────────────────────────────────

func TestBuildRunEvent_JobType_OmittedWhenFacetDisabled(t *testing.T) {
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringValue("SPARK"),
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if jobFacets(event).JobTypeJobFacet != nil {
		t.Error("expected jobType facet to be absent when FacetJobType is disabled")
	}
}

func TestBuildRunEvent_JobType_IncludedWhenFacetEnabled(t *testing.T) {
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringValue("SPARK"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)

	jt := jobFacets(BuildRunEvent(model, cap)).JobTypeJobFacet

	if jt == nil {
		t.Fatal("expected jobType facet to be present when FacetJobType is enabled")
	}
	if jt.ProcessingType != "BATCH" {
		t.Errorf("expected ProcessingType = %q, got %q", "BATCH", jt.ProcessingType)
	}
	if jt.Integration != "SPARK" {
		t.Errorf("expected Integration = %q, got %q", "SPARK", jt.Integration)
	}
}

func TestBuildRunEvent_JobType_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringNull(), // null — facet must be skipped
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)

	if jobFacets(BuildRunEvent(model, cap)).JobTypeJobFacet != nil {
		t.Error("expected jobType facet to be skipped when Integration is null")
	}
}

func TestBuildRunEvent_JobType_OptionalJobTypeField(t *testing.T) {
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringValue("AIRFLOW"),
		JobType:        types.StringValue("DAG"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)

	jt := jobFacets(BuildRunEvent(model, cap)).JobTypeJobFacet

	if jt == nil {
		t.Fatal("expected jobType facet to be present")
	}
	if jt.JobType == nil || *jt.JobType != "DAG" {
		t.Errorf("expected optional JobType = %q, got %v", "DAG", jt.JobType)
	}
}

func TestBuildRunEvent_Ownership_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.Ownership = &OwnershipJobModel{
		Owners: []JobOwnerModel{
			{Name: types.StringValue("team:data-engineering"), Type: types.StringValue("OWNER")},
		},
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobOwnership)

	ow := jobFacets(BuildRunEvent(model, cap)).OwnershipJobFacet

	if ow == nil {
		t.Fatal("expected ownership facet to be present")
	}
	if len(ow.Owners) != 1 || ow.Owners[0].Name != "team:data-engineering" {
		t.Errorf("expected owner name %q, got %v", "team:data-engineering", ow.Owners)
	}
}

func TestBuildRunEvent_Ownership_OmittedWhenDisabled(t *testing.T) {
	model := minimalModel()
	model.Ownership = &OwnershipJobModel{
		Owners: []JobOwnerModel{
			{Name: types.StringValue("team:data-engineering"), Type: types.StringValue("OWNER")},
		},
	}

	if jobFacets(BuildRunEvent(model, EmptyJobCapability())).OwnershipJobFacet != nil {
		t.Error("expected ownership facet to be absent when FacetJobOwnership is disabled")
	}
}

func TestBuildRunEvent_Documentation_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.Documentation = &DocumentationModel{
		Description: types.StringValue("My job docs"),
		ContentType: types.StringValue("text/markdown"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobDocumentation)

	doc := jobFacets(BuildRunEvent(model, cap)).DocumentationJobFacet

	if doc == nil {
		t.Fatal("expected documentation facet to be present")
	}
	if doc.Description != "My job docs" {
		t.Errorf("expected Description = %q, got %q", "My job docs", doc.Description)
	}
	if doc.ContentType == nil || *doc.ContentType != "text/markdown" {
		t.Errorf("expected ContentType = %q, got %v", "text/markdown", doc.ContentType)
	}
}

func TestBuildRunEvent_Documentation_SkippedWhenDescriptionIsNull(t *testing.T) {
	model := minimalModel()
	model.Documentation = &DocumentationModel{Description: types.StringNull()}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobDocumentation)

	if jobFacets(BuildRunEvent(model, cap)).DocumentationJobFacet != nil {
		t.Error("expected documentation facet to be skipped when description is null")
	}
}

func TestBuildRunEvent_SourceCode_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SourceCode = &SourceCodeJobModel{
		Language:   types.StringValue("Python"),
		SourceCode: types.StringValue("print('hello')"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSourceCode)

	sc := jobFacets(BuildRunEvent(model, cap)).SourceCodeJobFacet

	if sc == nil {
		t.Fatal("expected sourceCode facet to be present")
	}
	if sc.Language != "Python" {
		t.Errorf("expected Language = %q, got %q", "Python", sc.Language)
	}
}

func TestBuildRunEvent_SQL_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SQL = &SQLJobModel{Query: types.StringValue("SELECT 1")}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSQL)

	sf := jobFacets(BuildRunEvent(model, cap)).SQLJobFacet

	if sf == nil {
		t.Fatal("expected sql facet to be present")
	}
	if sf.Query != "SELECT 1" {
		t.Errorf("expected Query = %q, got %q", "SELECT 1", sf.Query)
	}
}

func TestBuildRunEvent_Tags_SkipsNullEntries(t *testing.T) {
	model := minimalModel()
	model.Tags = []TagsJobModel{
		{Name: types.StringValue("env"), Value: types.StringValue("prod")},
		{Name: types.StringNull(), Value: types.StringValue("orphaned")}, // null name → skip
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobTags)

	tf := jobFacets(BuildRunEvent(model, cap)).TagsJobFacet

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
	model.Tags = []TagsJobModel{
		{Name: types.StringNull(), Value: types.StringNull()},
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobTags)

	if jobFacets(BuildRunEvent(model, cap)).TagsJobFacet != nil {
		t.Error("expected tags facet to be absent when all entries have null name/value")
	}
}

func TestBuildRunEvent_SourceCodeLocation_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SourceCodeLocation = &SourceCodeLocationJobModel{
		Type:   types.StringValue("git"),
		URL:    types.StringValue("https://github.com/org/repo"),
		Branch: types.StringValue("main"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSourceCodeLocation)

	scl := jobFacets(BuildRunEvent(model, cap)).SourceCodeLocationJobFacet

	if scl == nil {
		t.Fatal("expected sourceCodeLocation facet to be present")
	}
	if scl.Type != "git" {
		t.Errorf("expected Type = %q, got %q", "git", scl.Type)
	}
	if scl.Branch == nil || *scl.Branch != "main" {
		t.Errorf("expected Branch = %q, got %v", "main", scl.Branch)
	}
}

func TestBuildRunEvent_SourceCodeLocation_SkippedWhenTypeIsNull(t *testing.T) {
	model := minimalModel()
	model.SourceCodeLocation = &SourceCodeLocationJobModel{
		Type: types.StringNull(),
		URL:  types.StringValue("https://github.com/org/repo"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSourceCodeLocation)

	if jobFacets(BuildRunEvent(model, cap)).SourceCodeLocationJobFacet != nil {
		t.Error("expected sourceCodeLocation to be skipped when Type is null")
	}
}

// ── BuildRunEvent — inputs / outputs ─────────────────────────────────────────

func TestBuildRunEvent_InputsPresent(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("ns"),
			Name:      types.StringValue("job"),
		},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bigquery"),
				Name:      types.StringValue("project.dataset.src"),
			}},
		},
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if len(event.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(event.Inputs))
	}
	if event.Inputs[0].Namespace != "bigquery" {
		t.Errorf("expected input namespace = %q, got %q", "bigquery", event.Inputs[0].Namespace)
	}
	if event.Inputs[0].Name != "project.dataset.src" {
		t.Errorf("expected input name = %q, got %q", "project.dataset.src", event.Inputs[0].Name)
	}
}

func TestBuildRunEvent_OutputsPresent(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("ns"),
			Name:      types.StringValue("job"),
		},
		Outputs: []OLOutputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bigquery"),
				Name:      types.StringValue("project.dataset.dst"),
			}},
		},
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if len(event.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(event.Outputs))
	}
	if event.Outputs[0].Name != "project.dataset.dst" {
		t.Errorf("expected output name = %q, got %q", "project.dataset.dst", event.Outputs[0].Name)
	}
}

func TestBuildRunEvent_MultipleInputsAndOutputs(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("ns"),
			Name:      types.StringValue("job"),
		},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{Namespace: types.StringValue("ns"), Name: types.StringValue("src1")}},
			{DatasetModel: DatasetModel{Namespace: types.StringValue("ns"), Name: types.StringValue("src2")}},
		},
		Outputs: []OLOutputModel{
			{DatasetModel: DatasetModel{Namespace: types.StringValue("ns"), Name: types.StringValue("dst1")}},
		},
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if len(event.Inputs) != 2 {
		t.Errorf("expected 2 inputs, got %d", len(event.Inputs))
	}
	if len(event.Outputs) != 1 {
		t.Errorf("expected 1 output, got %d", len(event.Outputs))
	}
}

// ── BuildRunEvent — dataset facets on inputs/outputs ─────────────────────────

func TestBuildRunEvent_Symlinks_IncludedOnInputWhenEnabled(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("hive"),
				Name:      types.StringValue("db.table"),
				Symlinks: []SymlinksDatasetModel{
					{Namespace: types.StringValue("bigquery"), Name: types.StringValue("project.dataset.table"), Type: types.StringValue("TABLE")},
				},
			}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetSymlinks)

	event := BuildRunEvent(model, cap)
	sl := inputFacets(event, 0).SymlinksDatasetFacet

	if sl == nil {
		t.Fatal("expected symlinks facet to be present")
	}
	if len(sl.Identifiers) != 1 || sl.Identifiers[0].Namespace != "bigquery" {
		t.Errorf("expected symlink namespace = %q, got %v", "bigquery", sl.Identifiers)
	}
}

func TestBuildRunEvent_Symlinks_OmittedOnInputWhenDisabled(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("hive"),
				Name:      types.StringValue("db.table"),
				Symlinks: []SymlinksDatasetModel{
					{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.table"), Type: types.StringValue("TABLE")},
				},
			}},
		},
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if inputFacets(event, 0).SymlinksDatasetFacet != nil {
		t.Error("expected symlinks facet to be absent when FacetDatasetSymlinks is disabled")
	}
}

func TestBuildRunEvent_ColumnLineage_IncludedOnOutputWhenEnabled(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.output")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("output_col"),
							InputFields: []InputFieldModel{
								{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.input"), Field: types.StringValue("input_col")},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	event := BuildRunEvent(model, cap)
	cl := outputFacets(event, 0).ColumnLineageDatasetFacet

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

func TestBuildRunEvent_ColumnLineage_OmittedOnOutputWhenDisabled(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{Name: types.StringValue("col"), InputFields: []InputFieldModel{
							{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("src_col")},
						}},
					},
				},
			},
		},
	}

	event := BuildRunEvent(model, EmptyJobCapability())

	if outputFacets(event, 0).ColumnLineageDatasetFacet != nil {
		t.Error("expected columnLineage to be absent when FacetDatasetColumnLineage is disabled")
	}
}

func TestBuildRunEvent_ColumnLineage_NotEmittedOnInputs(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in")}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	event := BuildRunEvent(model, cap)
	if len(event.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(event.Inputs))
	}
	// column_lineage is output-only — inputs have no ColumnLineageDatasetFacet
	if inputFacets(event, 0).ColumnLineageDatasetFacet != nil {
		t.Error("expected columnLineage to be absent on inputs")
	}
}

// ── buildColumnLineageFacet — duplicate field name aggregation ────────────────

func TestBuildRunEvent_ColumnLineage_DuplicateFieldNamesAreAggregated(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("out_col"),
							InputFields: []InputFieldModel{
								{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("col_a")},
							},
						},
						{
							Name: types.StringValue("out_col"), // same output column — must merge
							InputFields: []InputFieldModel{
								{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("col_b")},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	event := BuildRunEvent(model, cap)
	cl := outputFacets(event, 0).ColumnLineageDatasetFacet

	if cl == nil {
		t.Fatal("expected columnLineage facet to be present")
	}
	fv := cl.Fields["out_col"]
	if len(fv.InputFields) != 2 {
		t.Fatalf("expected 2 merged input fields, got %d", len(fv.InputFields))
	}
	fields := map[string]bool{fv.InputFields[0].Field: true, fv.InputFields[1].Field: true}
	if !fields["col_a"] {
		t.Error("expected col_a after aggregation")
	}
	if !fields["col_b"] {
		t.Error("expected col_b after aggregation")
	}
}

// ── buildTransformation ───────────────────────────────────────────────────────

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
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("out_col"),
							InputFields: []InputFieldModel{
								{
									Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("in_col"),
									Transformations: []TransformationModel{
										{Type: types.StringValue("DIRECT"), Subtype: types.StringValue("IDENTITY")},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	tr := getTransformation(t, BuildRunEvent(model, cap), 0, "out_col")

	if tr.Type != "DIRECT" {
		t.Errorf("expected Type = %q, got %q", "DIRECT", tr.Type)
	}
	if tr.Subtype == nil || *tr.Subtype != "IDENTITY" {
		t.Errorf("expected Subtype = %q, got %v", "IDENTITY", tr.Subtype)
	}
}

func TestBuildRunEvent_Transformation_MaskingFlag(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("masked_col"),
							InputFields: []InputFieldModel{
								{
									Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("pii_col"),
									Transformations: []TransformationModel{
										{Type: types.StringValue("INDIRECT"), Masking: types.BoolValue(true)},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	tr := getTransformation(t, BuildRunEvent(model, cap), 0, "masked_col")

	if tr.Masking == nil || !*tr.Masking {
		t.Errorf("expected Masking = true, got %v", tr.Masking)
	}
}

func TestBuildRunEvent_Transformation_NullOptionalFieldsOmitted(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("col"),
							InputFields: []InputFieldModel{
								{
									Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("src"),
									Transformations: []TransformationModel{
										{Type: types.StringValue("DIRECT"), Subtype: types.StringNull(), Description: types.StringNull(), Masking: types.BoolNull()},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	tr := getTransformation(t, BuildRunEvent(model, cap), 0, "col")

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

// ── buildStorageFacet — nil guard ─────────────────────────────────────────────

func TestBuildRunEvent_Storage_SkippedWhenStorageLayerIsNull(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"),
				Storage: &StorageDatasetModel{StorageLayer: types.StringNull(), FileFormat: types.StringValue("parquet")},
			}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetStorage)

	event := BuildRunEvent(model, cap)

	if inputFacets(event, 0).StorageDatasetFacet != nil {
		t.Error("expected storage facet to be skipped when StorageLayer is null")
	}
}

// ── buildCatalogFacet — nil guard ────────────────────────────────────────────

func TestBuildRunEvent_Catalog_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"),
				Catalog: &CatalogDatasetModel{
					Framework: types.StringNull(), // null required field → skip
					Type:      types.StringValue("hive"),
					Name:      types.StringValue("my-catalog"),
				},
			}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetCatalog)

	event := BuildRunEvent(model, cap)

	if inputFacets(event, 0).CatalogDatasetFacet != nil {
		t.Error("expected catalog facet to be skipped when Framework is null")
	}
}

// ── BuildDatasetEvent ─────────────────────────────────────────────────────────

func TestBuildDatasetEvent_ReturnsNonNil(t *testing.T) {
	model := &DatasetResourceModel{
		DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.table")},
	}

	if BuildDatasetEvent(model, EmptyDatasetCapability()) == nil {
		t.Fatal("BuildDatasetEvent returned nil")
	}
}

func TestBuildDatasetEvent_SetsNameAndNamespace(t *testing.T) {
	model := &DatasetResourceModel{
		DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("project.dataset.table")},
	}

	event := BuildDatasetEvent(model, EmptyDatasetCapability())

	if event.Dataset.Name != "project.dataset.table" {
		t.Errorf("expected Name = %q, got %q", "project.dataset.table", event.Dataset.Name)
	}
	if event.Dataset.Namespace != "bq" {
		t.Errorf("expected Namespace = %q, got %q", "bq", event.Dataset.Namespace)
	}
}

func TestBuildDatasetEvent_Schema_IncludedWhenEnabled(t *testing.T) {
	model := &DatasetResourceModel{
		DatasetModel: DatasetModel{
			Namespace: types.StringValue("bq"),
			Name:      types.StringValue("bq.table"),
			Schema: &SchemaDatasetModel{
				Fields: []SchemaFieldModel{
					{Name: types.StringValue("id"), Type: types.StringValue("INT64")},
					{Name: types.StringValue("name"), Type: types.StringValue("STRING")},
				},
			},
		},
	}
	cap := EmptyDatasetCapability().WithFacetEnabled(FacetDatasetSchema)

	event := BuildDatasetEvent(model, cap)

	if event.Dataset.Facets == nil || event.Dataset.Facets.SchemaDatasetFacet == nil {
		t.Fatal("expected schema facet to be present")
	}
	sf := event.Dataset.Facets.SchemaDatasetFacet
	if len(sf.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(sf.Fields))
	}
	if sf.Fields[0].Name != "id" {
		t.Errorf("expected first field name = %q, got %q", "id", sf.Fields[0].Name)
	}
	if sf.Fields[0].Type == nil || *sf.Fields[0].Type != "INT64" {
		t.Errorf("expected first field type = %q, got %v", "INT64", sf.Fields[0].Type)
	}
}
