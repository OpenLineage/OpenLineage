/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package file

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure OpenLineageProvider satisfies the provider.Provider interface.
var _ provider.Provider = &OpenLineageProvider{}

// OpenLineageProvider is the root provider struct.
// It holds only the version string; all runtime config lives in ProviderConfig.
type OpenLineageProvider struct {
	version string
}

// ProviderConfig is populated during Configure() and passed to every resource
// via resource.ConfigureRequest.ProviderData.
//
// Keeping it as a plain struct (not an interface) makes the type-assertion in
// ConsumerConfigure() explicit and easy to follow for new provider authors.
type ProviderConfig struct {
	OutputDir   string // absolute path to the directory where JSON files are written
	PrettyPrint bool   // true → indented JSON, false → compact JSON
}

// OpenLineageProviderModel mirrors the HCL provider block.
type OpenLineageProviderModel struct {
	OutputDir   types.String `tfsdk:"output_dir"`
	PrettyPrint types.Bool   `tfsdk:"pretty_print"`
}

// New returns the provider factory function expected by providerserver.Serve().
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &OpenLineageProvider{version: version}
	}
}

func (p *OpenLineageProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "openlineage"
	resp.Version = p.version
}

func (p *OpenLineageProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Terraform provider that records OpenLineage events as local JSON files. " +
			"Requires no external services or credentials — intended as a reference implementation " +
			"for OpenLineage provider developers.",
		Attributes: map[string]schema.Attribute{
			"output_dir": schema.StringAttribute{
				Required: true,
				Description: "Directory where lineage JSON files will be written. " +
					"One file per job: {output_dir}/{namespace}__{name}.json. " +
					"Created automatically if it does not exist.",
			},
			"pretty_print": schema.BoolAttribute{
				Optional: true,
				Description: "Write indented, human-readable JSON. " +
					"Defaults to true. Set to false for compact output.",
			},
		},
	}
}

func (p *OpenLineageProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config OpenLineageProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	outputDir := config.OutputDir.ValueString()
	if outputDir == "" {
		resp.Diagnostics.AddError(
			"Missing output_dir",
			"The output_dir provider attribute must be set to a writable directory path.",
		)
		return
	}

	// Ensure the output directory exists before any resource tries to write into it.
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		resp.Diagnostics.AddError(
			"Cannot create output_dir",
			"Unable to create directory '"+outputDir+"': "+err.Error(),
		)
		return
	}

	// Default pretty_print to true — readable output is friendlier for an example provider.
	prettyPrint := true
	if !config.PrettyPrint.IsNull() && !config.PrettyPrint.IsUnknown() {
		prettyPrint = config.PrettyPrint.ValueBool()
	}

	cfg := &ProviderConfig{
		OutputDir:   outputDir,
		PrettyPrint: prettyPrint,
	}

	// Passing the same pointer to both DataSourceData and ResourceData means
	// every resource receives it via resource.ConfigureRequest.ProviderData.
	resp.DataSourceData = cfg
	resp.ResourceData = cfg
}

func (p *OpenLineageProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewFileRunResource,     // openlineage_run     — emits RunEvent (job execution)
		NewFileJobResource,     // openlineage_job     — emits JobEvent (static job metadata)
		NewFileDatasetResource, // openlineage_dataset — emits DatasetEvent (dataset metadata)
	}
}

func (p *OpenLineageProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}
