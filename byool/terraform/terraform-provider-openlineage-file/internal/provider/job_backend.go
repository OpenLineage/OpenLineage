/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
)

// jobModel is the openlineage_job state: the base model plus this provider's file_path.
type jobModel struct {
	ol.JobResourceModel
	FilePath types.String `tfsdk:"file_path"`
}

// jobBackend writes each job's latest event to <output_dir>/jobs/<namespace>/<name>.json.
type jobBackend struct {
	fileBackend
}

var _ ol.JobResourceBackend = (*jobBackend)(nil)

// JobCapability is what this provider supports: every job facet except source_code, and every
// dataset facet except catalog and fabric_one_lake. The exceptions are only there to show that
// configs using disabled facets are still accepted (the values are stored, not written).
func JobCapability() ol.JobCapability {
	c := ol.EmptyJobCapability()
	for _, s := range ol.JobFacetSchemas {
		if s.JobFacet != ol.SourceCodeJobFacet {
			c = c.WithFacetEnabled(s.JobFacet)
		}
	}
	return c.WithDatasetFacetEnabled(supportedDatasetFacets()...)
}

func (b *jobBackend) Capability() ol.JobCapability { return JobCapability() }

func (b *jobBackend) NewModel() any { return &jobModel{} }

func (b *jobBackend) ConsumerEmit(_ context.Context, model any) diag.Diagnostics {
	diags := b.configured()
	if diags.HasError() {
		return diags
	}
	m := model.(*jobModel)
	builder := ol.NewJobEventBuilder(&diags, b.Capability(), b.cfg.Producer)
	var event any
	if b.cfg.JobEventType == jobEventTypeRun {
		event = builder.BuildRunEvent(&m.JobResourceModel)
	} else {
		event = builder.BuildJobEvent(&m.JobResourceModel)
	}
	if diags.HasError() {
		return diags
	}
	path := b.eventPath("jobs", m.Namespace.ValueString(), m.Name.ValueString())
	diags.Append(writeEvent(path, event)...)
	m.FilePath = types.StringValue(path)
	return diags
}

func (b *jobBackend) ConsumerRead(_ context.Context, model any) (bool, diag.Diagnostics) {
	return fileExists(model.(*jobModel).FilePath.ValueString())
}

func (b *jobBackend) ConsumerDelete(_ context.Context, model any) diag.Diagnostics {
	return removeFile(model.(*jobModel).FilePath.ValueString())
}
