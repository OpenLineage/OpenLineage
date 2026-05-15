/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"testing"

	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/hashicorp/terraform-plugin-framework/diag"
)

// ── BuildRunEvent — event structure ──────────────────────────────────────────

func TestBuildRunEvent_ReturnsNonNil(t *testing.T) {
	if BuildRunEvent(minimalModel(), EmptyJobCapability()) == nil {
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
	if BuildRunEvent(minimalModel(), EmptyJobCapability()).Run.RunID == "" {
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

// ── BuildRunEvent — inputs / outputs ─────────────────────────────────────────

func TestBuildRunEvent_InputsPresent(t *testing.T) {
	model := simpleJobModel()
	model.Inputs = olInputs(datasetModel("bigquery", "project.dataset.src"))

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
	model := simpleJobModel()
	model.Outputs = olOutputs(datasetModel("bigquery", "project.dataset.dst"))

	event := BuildRunEvent(model, EmptyJobCapability())

	if len(event.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(event.Outputs))
	}
	if event.Outputs[0].Name != "project.dataset.dst" {
		t.Errorf("expected output name = %q, got %q", "project.dataset.dst", event.Outputs[0].Name)
	}
}

func TestBuildRunEvent_MultipleInputsAndOutputs(t *testing.T) {
	model := simpleJobModel()
	model.Inputs = olInputs(datasetModel("ns", "src1"), datasetModel("ns", "src2"))
	model.Outputs = olOutputs(datasetModel("ns", "dst1"))

	event := BuildRunEvent(model, EmptyJobCapability())

	if len(event.Inputs) != 2 {
		t.Errorf("expected 2 inputs, got %d", len(event.Inputs))
	}
	if len(event.Outputs) != 1 {
		t.Errorf("expected 1 output, got %d", len(event.Outputs))
	}
}

// ── BuildRunEvent — smoke tests ───────────────────────────────────────────────

func TestBuildRunEvent_AllFacetsEnabled_MinimalModel(t *testing.T) {
	// All facets enabled but model only has namespace+name — every facet block is nil.
	// The builder must not panic and must produce no diagnostics.
	var diags diag.Diagnostics
	event := NewJobEventBuilder(&diags, fullJobCapability()).BuildRunEvent(minimalModel())

	if diags.HasError() {
		t.Errorf("expected no diagnostics for minimal model with all facets enabled, got: %v", diags)
	}
	if event == nil {
		t.Fatal("expected non-nil event")
	}
	if jobFacets(event).JobTypeJobFacet != nil {
		t.Error("expected nil job_type facet when model block is absent")
	}
}

func TestBuildRunEvent_AllFacetsDisabled_FullModel(t *testing.T) {
	// Every facet block is populated but all facets are disabled.
	// No facets should be emitted, no diagnostics, no panic.
	var diags diag.Diagnostics
	event := NewJobEventBuilder(&diags, EmptyJobCapability()).BuildRunEvent(fullModel())

	if diags.HasError() {
		t.Errorf("expected no diagnostics, got: %v", diags)
	}
	if event.Job.Facets != nil {
		t.Error("expected nil job facets when all facets are disabled")
	}
	if len(event.Inputs) != 1 {
		t.Fatalf("expected 1 input, got %d", len(event.Inputs))
	}
	if event.Inputs[0].Facets != nil {
		t.Error("expected nil input dataset facets when all facets are disabled")
	}
	if len(event.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(event.Outputs))
	}
	if event.Outputs[0].Facets != nil {
		t.Error("expected nil output dataset facets when all facets are disabled")
	}
}

// ── BuildDatasetEvent ─────────────────────────────────────────────────────────

func TestBuildDatasetEvent_ReturnsNonNil(t *testing.T) {
	model := &DatasetResourceModel{DatasetModel: datasetModel("bq", "bq.table")}

	if BuildDatasetEvent(model, EmptyDatasetCapability()) == nil {
		t.Fatal("BuildDatasetEvent returned nil")
	}
}

func TestBuildDatasetEvent_SetsNameAndNamespace(t *testing.T) {
	model := &DatasetResourceModel{DatasetModel: datasetModel("bq", "project.dataset.table")}

	event := BuildDatasetEvent(model, EmptyDatasetCapability())

	if event.Dataset.Name != "project.dataset.table" {
		t.Errorf("expected Name = %q, got %q", "project.dataset.table", event.Dataset.Name)
	}
	if event.Dataset.Namespace != "bq" {
		t.Errorf("expected Namespace = %q, got %q", "bq", event.Dataset.Namespace)
	}
}
