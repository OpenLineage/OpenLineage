/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"encoding/json"
	"strings"
	"testing"

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

// asJSON marshals event.AsEmittable() to a JSON string.
// Fails the test immediately if marshaling fails.
func asJSON(t *testing.T, event interface {
	AsEmittable() openlineage.Event
}) string {
	t.Helper()
	data, err := json.Marshal(event.AsEmittable())
	if err != nil {
		t.Fatalf("json.Marshal(event.AsEmittable()) failed: %v", err)
	}
	return string(data)
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

func TestBuildRunEvent_SetsSchemaURLOnEmittable(t *testing.T) {
	event := BuildRunEvent(minimalModel(), EmptyJobCapability())
	js := asJSON(t, event)

	if !strings.Contains(js, "schemaURL") {
		t.Error("expected serialized event to contain 'schemaURL'")
	}
	if !strings.Contains(js, "openlineage") {
		t.Error("expected schemaURL to reference openlineage")
	}
}

func TestBuildRunEvent_EventTypeIsComplete(t *testing.T) {
	js := asJSON(t, BuildRunEvent(minimalModel(), EmptyJobCapability()))

	if !strings.Contains(js, "COMPLETE") {
		t.Error("expected eventType COMPLETE in serialized event")
	}
}

// ── BuildRunEvent — job facet filtering ──────────────────────────────────────

func TestBuildRunEvent_JobType_OmittedWhenFacetDisabled(t *testing.T) {
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringValue("SPARK"),
	}

	js := asJSON(t, BuildRunEvent(model, EmptyJobCapability())) // FacetJobType NOT enabled

	if strings.Contains(js, "jobType") {
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "jobType") {
		t.Error("expected jobType facet in serialized event when FacetJobType is enabled")
	}
	if !strings.Contains(js, "BATCH") {
		t.Error("expected processingType BATCH in serialized event")
	}
	if !strings.Contains(js, "SPARK") {
		t.Error("expected integration SPARK in serialized event")
	}
}

func TestBuildRunEvent_JobType_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	// Null Integration — facet must be silently skipped, not emit empty string.
	model := minimalModel()
	model.JobType = &JobTypeJobModel{
		ProcessingType: types.StringValue("BATCH"),
		Integration:    types.StringNull(), // null — not yet known
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobType)

	js := asJSON(t, BuildRunEvent(model, cap))

	if strings.Contains(js, "jobType") {
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "DAG") {
		t.Error("expected optional job_type=DAG in serialized event")
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "ownership") {
		t.Error("expected ownership facet in serialized event")
	}
	if !strings.Contains(js, "team:data-engineering") {
		t.Error("expected owner name in serialized event")
	}
}

func TestBuildRunEvent_Ownership_OmittedWhenDisabled(t *testing.T) {
	model := minimalModel()
	model.Ownership = &OwnershipJobModel{
		Owners: []JobOwnerModel{
			{Name: types.StringValue("team:data-engineering"), Type: types.StringValue("OWNER")},
		},
	}

	js := asJSON(t, BuildRunEvent(model, EmptyJobCapability()))

	if strings.Contains(js, "ownership") {
		t.Error("expected ownership facet to be absent when FacetJobOwnership is disabled")
	}
}

func TestBuildRunEvent_Documentation_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.Documentation = &DocumentationJobModel{
		Description: types.StringValue("My job docs"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobDocumentation)

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "documentation") {
		t.Error("expected documentation facet in serialized event")
	}
	if !strings.Contains(js, "My job docs") {
		t.Error("expected description text in serialized event")
	}
}

func TestBuildRunEvent_Documentation_SkippedWhenDescriptionIsNull(t *testing.T) {
	model := minimalModel()
	model.Documentation = &DocumentationJobModel{
		Description: types.StringNull(),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobDocumentation)

	js := asJSON(t, BuildRunEvent(model, cap))

	if strings.Contains(js, "documentation") {
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "sourceCode") {
		t.Error("expected sourceCode facet in serialized event")
	}
}

func TestBuildRunEvent_SQL_IncludedWhenEnabled(t *testing.T) {
	model := minimalModel()
	model.SQL = &SQLJobModel{Query: types.StringValue("SELECT 1")}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSQL)

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "sql") {
		t.Error("expected sql facet in serialized event")
	}
	if !strings.Contains(js, "SELECT 1") {
		t.Error("expected SQL query in serialized event")
	}
}

func TestBuildRunEvent_Tags_SkipsNullEntries(t *testing.T) {
	model := minimalModel()
	model.Tags = []TagsJobModel{
		{Name: types.StringValue("env"), Value: types.StringValue("prod")},
		{Name: types.StringNull(), Value: types.StringValue("orphaned")}, // null name → skip
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobTags)

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "env") {
		t.Error("expected valid tag 'env' in serialized event")
	}
	if strings.Contains(js, "orphaned") {
		t.Error("expected tag with null name to be skipped")
	}
}

func TestBuildRunEvent_Tags_OmittedWhenAllEntriesAreNull(t *testing.T) {
	model := minimalModel()
	model.Tags = []TagsJobModel{
		{Name: types.StringNull(), Value: types.StringNull()},
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobTags)

	js := asJSON(t, BuildRunEvent(model, cap))

	// tags facet should not appear if all entries were skipped
	if strings.Contains(js, `"tags"`) {
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "sourceCodeLocation") {
		t.Error("expected sourceCodeLocation facet in serialized event")
	}
	if !strings.Contains(js, "main") {
		t.Error("expected branch name in serialized event")
	}
}

func TestBuildRunEvent_SourceCodeLocation_SkippedWhenTypeIsNull(t *testing.T) {
	model := minimalModel()
	model.SourceCodeLocation = &SourceCodeLocationJobModel{
		Type: types.StringNull(), // required — null → skip
		URL:  types.StringValue("https://github.com/org/repo"),
	}
	cap := EmptyJobCapability().WithFacetEnabled(FacetJobSourceCodeLocation)

	js := asJSON(t, BuildRunEvent(model, cap))

	if strings.Contains(js, "sourceCodeLocation") {
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
		OLJobConfig: OLJobConfig{
			Namespace: types.StringValue("ns"),
			Name:      types.StringValue("job"),
		},
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "symlinks") {
		t.Error("expected symlinks facet in serialized event")
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

	js := asJSON(t, BuildRunEvent(model, EmptyJobCapability()))

	if strings.Contains(js, "symlinks") {
		t.Error("expected symlinks facet to be absent when FacetDatasetSymlinks is disabled")
	}
}

func TestBuildRunEvent_ColumnLineage_IncludedOnOutputWhenEnabled(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{
					Namespace: types.StringValue("bq"),
					Name:      types.StringValue("bq.output"),
				},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("output_col"),
							InputFields: []InputFieldModel{
								{
									Namespace: types.StringValue("bq"),
									Name:      types.StringValue("bq.input"),
									Field:     types.StringValue("input_col"),
								},
							},
						},
					},
				},
			},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "columnLineage") {
		t.Error("expected columnLineage facet in serialized event")
	}
	if !strings.Contains(js, "output_col") {
		t.Error("expected output column name in serialized event")
	}
	if !strings.Contains(js, "input_col") {
		t.Error("expected input column name in serialized event")
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

	js := asJSON(t, BuildRunEvent(model, EmptyJobCapability()))

	if strings.Contains(js, "columnLineage") {
		t.Error("expected columnLineage to be absent when FacetDatasetColumnLineage is disabled")
	}
}

func TestBuildRunEvent_ColumnLineage_NotEmittedOnInputs(t *testing.T) {
	// column_lineage is output-only per OL spec — inputs don't carry it.
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in")}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetColumnLineage)

	// no panic, no columnLineage on inputs (the model doesn't have it on inputs)
	event := BuildRunEvent(model, cap)
	if len(event.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(event.Inputs))
	}
}

// ── buildColumnLineageFacet — duplicate field name aggregation ────────────────

func TestBuildRunEvent_ColumnLineage_DuplicateFieldNamesAreAggregated(t *testing.T) {
	// Two fields blocks with the same output column name must be merged,
	// not silently overwritten — the spec says InputFields is a list.
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Outputs: []OLOutputModel{
			{
				DatasetModel: DatasetModel{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.out")},
				ColumnLineage: &ColumnLineageDatasetModel{
					Fields: []ColumnLineageFieldModel{
						{
							Name: types.StringValue("out_col"), // first block for out_col
							InputFields: []InputFieldModel{
								{Namespace: types.StringValue("bq"), Name: types.StringValue("bq.in"), Field: types.StringValue("col_a")},
							},
						},
						{
							Name: types.StringValue("out_col"), // second block, same output column
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

	js := asJSON(t, BuildRunEvent(model, cap))

	// Both input columns must appear — not just col_b (which would indicate overwrite).
	if !strings.Contains(js, "col_a") {
		t.Error("expected col_a to be present after duplicate field name aggregation")
	}
	if !strings.Contains(js, "col_b") {
		t.Error("expected col_b to be present after duplicate field name aggregation")
	}
}

// ── buildTransformation ───────────────────────────────────────────────────────

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
									Namespace: types.StringValue("bq"),
									Name:      types.StringValue("bq.in"),
									Field:     types.StringValue("in_col"),
									Transformation: &TransformationModel{
										Type:    types.StringValue("DIRECT"),
										Subtype: types.StringValue("IDENTITY"),
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "DIRECT") {
		t.Error("expected transformation type DIRECT in serialized event")
	}
	if !strings.Contains(js, "IDENTITY") {
		t.Error("expected transformation subtype IDENTITY in serialized event")
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
									Namespace: types.StringValue("bq"),
									Name:      types.StringValue("bq.in"),
									Field:     types.StringValue("pii_col"),
									Transformation: &TransformationModel{
										Type:    types.StringValue("INDIRECT"),
										Masking: types.BoolValue(true),
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

	js := asJSON(t, BuildRunEvent(model, cap))

	if !strings.Contains(js, "masking") {
		t.Error("expected masking field in serialized transformation")
	}
}

func TestBuildRunEvent_Transformation_NullOptionalFieldsOmitted(t *testing.T) {
	// Subtype, Description, Masking are optional — null values must not emit empty strings.
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
									Namespace: types.StringValue("bq"),
									Name:      types.StringValue("bq.in"),
									Field:     types.StringValue("src"),
									Transformation: &TransformationModel{
										Type:        types.StringValue("DIRECT"),
										Subtype:     types.StringNull(),
										Description: types.StringNull(),
										Masking:     types.BoolNull(),
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

	js := asJSON(t, BuildRunEvent(model, cap))

	// 'subtype' and 'description' should not appear as null or empty string
	if strings.Contains(js, `"subtype":null`) {
		t.Error("expected null subtype to be omitted, not serialized as null")
	}
	if strings.Contains(js, `"description":null`) {
		t.Error("expected null description to be omitted, not serialized as null")
	}
}

// ── buildStorageFacet — nil guard ─────────────────────────────────────────────

func TestBuildRunEvent_Storage_SkippedWhenStorageLayerIsNull(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bq"),
				Name:      types.StringValue("bq.in"),
				Storage: &StorageDatasetModel{
					StorageLayer: types.StringNull(), // null — skip facet
					FileFormat:   types.StringValue("parquet"),
				},
			}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetStorage)

	js := asJSON(t, BuildRunEvent(model, cap))

	if strings.Contains(js, "storage") {
		t.Error("expected storage facet to be skipped when StorageLayer is null")
	}
}

// ── buildCatalogFacet — nil guard ────────────────────────────────────────────

func TestBuildRunEvent_Catalog_SkippedWhenRequiredFieldIsNull(t *testing.T) {
	model := &JobResourceModel{
		OLJobConfig: OLJobConfig{Namespace: types.StringValue("ns"), Name: types.StringValue("job")},
		Inputs: []OLInputModel{
			{DatasetModel: DatasetModel{
				Namespace: types.StringValue("bq"),
				Name:      types.StringValue("bq.in"),
				Catalog: &CatalogDatasetModel{
					Framework: types.StringNull(), // null required field → skip whole facet
					Type:      types.StringValue("hive"),
					Name:      types.StringValue("my-catalog"),
				},
			}},
		},
	}
	cap := EmptyJobCapability().WithDatasetFacetEnabled(FacetDatasetCatalog)

	js := asJSON(t, BuildRunEvent(model, cap))

	if strings.Contains(js, "catalog") {
		t.Error("expected catalog facet to be skipped when Framework is null")
	}
}

// ── BuildDatasetEvent ─────────────────────────────────────────────────────────

func TestBuildDatasetEvent_ReturnsNonNil(t *testing.T) {
	model := &DatasetResourceModel{
		DatasetModel: DatasetModel{
			Namespace: types.StringValue("bq"),
			Name:      types.StringValue("bq.table"),
		},
	}

	event := BuildDatasetEvent(model, EmptyDatasetCapability())

	if event == nil {
		t.Fatal("BuildDatasetEvent returned nil")
	}
}

func TestBuildDatasetEvent_SetsNameAndNamespace(t *testing.T) {
	model := &DatasetResourceModel{
		DatasetModel: DatasetModel{
			Namespace: types.StringValue("bq"),
			Name:      types.StringValue("project.dataset.table"),
		},
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
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	js := string(data)

	if !strings.Contains(js, "schema") {
		t.Error("expected schema facet in serialized dataset event")
	}
	if !strings.Contains(js, "INT64") {
		t.Error("expected column type INT64 in serialized dataset event")
	}
}
