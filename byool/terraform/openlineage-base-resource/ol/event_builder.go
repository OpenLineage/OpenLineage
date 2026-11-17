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
		requireString(e.Diagnostics, data.Name, base.AtName("name")),
		requireString(e.Diagnostics, data.Namespace, base.AtName("namespace")),
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
		requireString(e.Diagnostics, data.Name, base.AtName("name")),
		requireString(e.Diagnostics, data.Namespace, base.AtName("namespace")),
		producer,
		facetsList...,
	)
}

// ── Job facets ────────────────────────────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildJobFacets(data *OLJobConfig, base path.Path) []facets.JobFacet {
	type entry struct {
		facet   JobFacet
		present bool // evaluated before the pointer is boxed into the interface
		model   JobFacetBuilder
		pathKey string
	}

	entries := []entry{
		{FacetJobType, data.JobType != nil, data.JobType, "job_type"},
		{FacetJobOwnership, data.Ownership != nil, data.Ownership, "ownership"},
		{FacetJobDocumentation, data.Documentation != nil, data.Documentation, "documentation"},
		{FacetJobSourceCode, data.SourceCode != nil, data.SourceCode, "source_code"},
		{FacetJobSourceCodeLocation, data.SourceCodeLocation != nil, data.SourceCodeLocation, "source_code_location"},
		{FacetJobSQL, data.SQL != nil, data.SQL, "sql"},
		{FacetJobTags, data.Tags != nil, data.Tags, "tags"},
	}

	var fs []facets.JobFacet
	for _, en := range entries {
		if e.cap.isJobEnabled(en.facet) && en.present {
			if f := en.model.BuildJobFacet(producer, e.Diagnostics, base.AtName(en.pathKey)); f != nil {
				fs = append(fs, f)
			}
		}
	}
	return fs
}

// ── Dataset / input / output builders ────────────────────────────────────────

func (e *OpenLineageEventBuilder) buildInputElement(input *OLInputModel, base path.Path) openlineage.InputElement {
	ie := openlineage.NewInputElement(
		requireString(e.Diagnostics, input.Name, base.AtName("name")),
		requireString(e.Diagnostics, input.Namespace, base.AtName("namespace")),
	)
	ie = ie.WithFacets(e.buildDatasetFacets(&input.DatasetModel, base)...)
	return ie
}

func (e *OpenLineageEventBuilder) buildOutputElement(output *OLOutputModel, base path.Path) openlineage.OutputElement {
	oe := openlineage.NewOutputElement(
		requireString(e.Diagnostics, output.Name, base.AtName("name")),
		requireString(e.Diagnostics, output.Namespace, base.AtName("namespace")),
	)
	oe = oe.WithFacets(e.buildDatasetFacets(&output.DatasetModel, base)...)

	if e.cap.isDatasetEnabled(FacetDatasetColumnLineage) && output.ColumnLineage != nil {
		oe = oe.WithFacets(output.ColumnLineage.BuildDatasetFacet(producer, e.Diagnostics, base.AtName("column_lineage")))
	}
	return oe
}

func (e *OpenLineageEventBuilder) buildDatasetFacets(dataset *DatasetModel, base path.Path) []facets.DatasetFacet {
	type entry struct {
		facet   DatasetFacet
		present bool // evaluated before the pointer is boxed into the interface
		model   DatasetFacetBuilder
		pathKey string
	}

	entries := []entry{
		{FacetDatasetSymlinks, dataset.Symlinks != nil, dataset.Symlinks, "symlinks"},
		{FacetDatasetSchema, dataset.Schema != nil, dataset.Schema, "schema"},
		{FacetDatasetDataSource, dataset.DataSource != nil, dataset.DataSource, "data_source"},
		{FacetDatasetDocumentation, dataset.Documentation != nil, dataset.Documentation, "documentation"},
		{FacetDatasetType, dataset.DatasetType != nil, dataset.DatasetType, "dataset_type"},
		{FacetDatasetVersion, dataset.Version != nil, dataset.Version, "version"},
		{FacetDatasetStorage, dataset.Storage != nil, dataset.Storage, "storage"},
		{FacetDatasetOwnership, dataset.Ownership != nil, dataset.Ownership, "ownership"},
		{FacetDatasetLifecycleStateChange, dataset.LifecycleStateChange != nil, dataset.LifecycleStateChange, "lifecycle_state_change"},
		{FacetDatasetHierarchy, dataset.Hierarchy != nil, dataset.Hierarchy, "hierarchy"},
		{FacetDatasetCatalog, dataset.Catalog != nil, dataset.Catalog, "catalog"},
		{FacetDatasetTags, dataset.Tags != nil, dataset.Tags, "tags"},
	}

	var fs []facets.DatasetFacet
	for _, en := range entries {
		if e.cap.isDatasetEnabled(en.facet) && en.present {
			if f := en.model.BuildDatasetFacet(producer, e.Diagnostics, base.AtName(en.pathKey)); f != nil {
				fs = append(fs, f)
			}
		}
	}
	return fs
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// requireString returns the string value of attr. If it is null or unknown,
// an attribute error is added to diags with the supplied path.
func requireString(diags *diag.Diagnostics, attr types.String, p path.Path) string {
	if attr.IsNull() || attr.IsUnknown() {
		diags.AddAttributeError(p, "Missing required attribute",
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
