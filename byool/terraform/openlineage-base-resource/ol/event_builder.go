/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"fmt"

	"github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// OpenLineageEventBuilder assembles OpenLineage events from Terraform models.
// All build methods are receivers so they share the Diagnostics sink and capability,
// and can attach precise path-aware errors for missing required attributes.
type OpenLineageEventBuilder struct {
	Diagnostics *diag.Diagnostics
	cap         Capability
	producer    string
}

// NewJobEventBuilder creates a builder for job events (and their input/output datasets).
func NewJobEventBuilder(diags *diag.Diagnostics, cap JobCapability, producer string) *OpenLineageEventBuilder {
	return &OpenLineageEventBuilder{Diagnostics: diags, cap: cap.Capability, producer: producer}
}

// NewDatasetEventBuilder creates a builder for standalone dataset events.
func NewDatasetEventBuilder(diags *diag.Diagnostics, cap DatasetCapability, producer string) *OpenLineageEventBuilder {
	return &OpenLineageEventBuilder{Diagnostics: diags, cap: cap.Capability, producer: producer}
}

// ── Package-level convenience wrappers ───────────────────────────────────────

// BuildRunEvent is a package-level convenience wrapper around
// NewJobEventBuilder(...).BuildRunEvent(...).
func BuildRunEvent(data *JobResourceModel, cap JobCapability, producer string) *openlineage.RunEvent {
	var diags diag.Diagnostics
	return NewJobEventBuilder(&diags, cap, producer).BuildRunEvent(data)
}

// BuildJobEvent is a package-level convenience wrapper around
// NewJobEventBuilder(...).BuildJobEvent(...).
func BuildJobEvent(data *JobResourceModel, cap JobCapability, producer string) *openlineage.JobEvent {
	var diags diag.Diagnostics
	return NewJobEventBuilder(&diags, cap, producer).BuildJobEvent(data)
}

// BuildDatasetEvent is a package-level convenience wrapper around
// NewDatasetEventBuilder(...).BuildDatasetEvent(...).
func BuildDatasetEvent(data *DatasetResourceModel, cap DatasetCapability, producer string) *openlineage.DatasetEvent {
	var diags diag.Diagnostics
	return NewDatasetEventBuilder(&diags, cap, producer).BuildDatasetEvent(data)
}

// ── Top-level event builders ──────────────────────────────────────────────────

// BuildRunEvent wraps a JobEvent with run-specific fields (event type + run ID).
func (e *OpenLineageEventBuilder) BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent {
	jobEvent := e.BuildJobEvent(data)
	runID := uuid.New()

	run := openlineage.NewRunEvent(openlineage.EventTypeComplete, runID, jobEvent.Job.Name, e.producer)
	run.Job = jobEvent.Job
	run.Inputs = jobEvent.Inputs
	run.Outputs = jobEvent.Outputs
	run.EventTime = jobEvent.EventTime
	return run
}

// BuildJobEvent assembles an OpenLineage JobEvent from the Terraform job model.
func (e *OpenLineageEventBuilder) BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent {
	base := path.Empty()

	event := openlineage.NewJobEvent(
		requiredStringValue(e.Diagnostics, data.Name, base.AtName("name")),
		requiredStringValue(e.Diagnostics, data.Namespace, base.AtName("namespace")),
		e.producer,
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
		requiredStringValue(e.Diagnostics, data.Name, base.AtName("name")),
		requiredStringValue(e.Diagnostics, data.Namespace, base.AtName("namespace")),
		e.producer,
		facetsList...,
	)
}

// ── Dataset / input / output builders ────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildInputElement(input *OLInputModel, base path.Path) openlineage.InputElement {
	ie := openlineage.NewInputElement(
		requiredStringValue(e.Diagnostics, input.Name, base.AtName("name")),
		requiredStringValue(e.Diagnostics, input.Namespace, base.AtName("namespace")),
	)
	ie = ie.WithFacets(e.buildDatasetFacets(&input.DatasetModel, base)...)
	return ie
}

func (e *OpenLineageEventBuilder) buildOutputElement(output *OLOutputModel, base path.Path) openlineage.OutputElement {
	oe := openlineage.NewOutputElement(
		requiredStringValue(e.Diagnostics, output.Name, base.AtName("name")),
		requiredStringValue(e.Diagnostics, output.Namespace, base.AtName("namespace")),
	)
	oe = oe.WithFacets(e.buildDatasetFacets(&output.DatasetModel, base)...)

	if e.cap.IsDatasetEnabled(ColumnLineageDatasetFacet) && output.ColumnLineage != nil {
		oe = oe.WithFacets(output.ColumnLineage.BuildDatasetFacet(e.producer, e.Diagnostics, base.AtName("column_lineage")))
	}
	return oe
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func requiredStringValue(diags *diag.Diagnostics, attr types.String, p path.Path) string {
	if attr.IsNull() || attr.IsUnknown() {
		diags.AddAttributeError(p, "Missing required attribute",
			fmt.Sprintf("Attribute %q must be set. The attribute is validated in config so it shouldn't happen", p))
	}
	return attr.ValueString()
}

func requireBoolValue(diags *diag.Diagnostics, attr types.Bool, p path.Path) bool {
	if attr.IsNull() || attr.IsUnknown() {
		diags.AddAttributeError(p, "Missing required attribute",
			fmt.Sprintf("Attribute %q must be set. The attribute is validated in config so it shouldn't happen", p))
	}
	return attr.ValueBool()
}

func requireInt64Value(diags *diag.Diagnostics, attr types.Int64, p path.Path) int64 {
	if attr.IsNull() || attr.IsUnknown() {
		diags.AddAttributeError(p, "Missing required attribute",
			fmt.Sprintf("Attribute %q must be set. The attribute is validated in config so it shouldn't happen", p))
	}
	return attr.ValueInt64()
}

func requireFloat64Value(diags *diag.Diagnostics, attr types.Float64, p path.Path) float64 {
	if attr.IsNull() || attr.IsUnknown() {
		diags.AddAttributeError(p, "Missing required attribute",
			fmt.Sprintf("Attribute %q must be set. The attribute is validated in config so it shouldn't happen", p))
	}
	return attr.ValueFloat64()
}

func isKnownString(v types.String) bool {
	return !v.IsNull() && !v.IsUnknown()
}

func optionalStringValue(v types.String) *string {
	if !isKnownString(v) {
		return nil
	}
	val := v.ValueString()
	return &val
}

func optionalBoolValue(v types.Bool) *bool {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	val := v.ValueBool()
	return &val
}

func optionalInt64Value(v types.Int64) *int64 {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	val := v.ValueInt64()
	return &val
}

func optionalFloat64Value(v types.Float64) *float64 {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	val := v.ValueFloat64()
	return &val
}
