/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package provider implements the example "openlineage" provider: openlineage_job and
// openlineage_dataset resources that write their latest OpenLineage event to a JSON file.
package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
)

// DefaultProducer is the producer URI written into events unless the provider config sets one.
const DefaultProducer = "https://github.com/OpenLineage/OpenLineage/tree/main/byool/terraform/terraform-provider-openlineage-file"

const (
	jobEventTypeJob = "job"
	jobEventTypeRun = "run"
)

// Config is the resolved provider configuration, passed to resources as ProviderData.
type Config struct {
	OutputDir    string
	Producer     string
	JobEventType string // jobEventTypeJob or jobEventTypeRun
}

type providerModel struct {
	OutputDir    types.String `tfsdk:"output_dir"`
	Producer     types.String `tfsdk:"producer"`
	JobEventType types.String `tfsdk:"job_event_type"`
}

type fileProvider struct {
	version string
}

var _ provider.Provider = (*fileProvider)(nil)

// New returns a provider factory for providerserver.Serve and tests.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &fileProvider{version: version}
	}
}

func (p *fileProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "openlineage"
	resp.Version = p.version
}

func (p *fileProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Example OpenLineage provider: writes the latest event of each job and dataset to a JSON file.",
		Attributes: map[string]schema.Attribute{
			"output_dir": schema.StringAttribute{
				Required:    true,
				Description: "Directory the event files are written to (jobs/<namespace>/<name>.json, datasets/<namespace>/<name>.json).",
			},
			"producer": schema.StringAttribute{
				Optional:    true,
				Description: "Producer URI written into events. Defaults to this provider's source URL.",
			},
			"job_event_type": schema.StringAttribute{
				Optional: true,
				Description: `Event written for jobs: "job" (default) writes a JobEvent; "run" wraps it in a COMPLETE ` +
					"RunEvent with a new run ID on every apply, for consumers that only accept run events.",
				Validators: []validator.String{stringvalidator.OneOf(jobEventTypeJob, jobEventTypeRun)},
			},
		},
	}
}

func (p *fileProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var m providerModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	cfg := &Config{
		OutputDir:    m.OutputDir.ValueString(),
		Producer:     DefaultProducer,
		JobEventType: jobEventTypeJob,
	}
	if !m.Producer.IsNull() && !m.Producer.IsUnknown() {
		cfg.Producer = m.Producer.ValueString()
	}
	if !m.JobEventType.IsNull() && !m.JobEventType.IsUnknown() {
		cfg.JobEventType = m.JobEventType.ValueString()
	}
	resp.ResourceData = cfg
}

func (p *fileProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		func() resource.Resource { return ol.NewJobResource(&jobBackend{}) },
		func() resource.Resource { return ol.NewDatasetResource(&datasetBackend{}) },
	}
}

func (p *fileProvider) DataSources(context.Context) []func() datasource.DataSource {
	return nil
}
