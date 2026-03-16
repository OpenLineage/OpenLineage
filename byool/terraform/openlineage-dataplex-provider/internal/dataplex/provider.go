/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package dataplex

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure OpenLineageProvider satisfies various dataplex interfaces.
var _ provider.Provider = &OpenLineageProvider{}

// OpenLineageProvider defines the dataplex implementation.
type OpenLineageProvider struct {
	// version is set to the dataplex version on release, "dev" when the
	// dataplex is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// OpenLineageProviderModel describes the dataplex data model.
type OpenLineageProviderModel struct {
	ProjectID       types.String `tfsdk:"project_id"`
	Region          types.String `tfsdk:"region"`
	CredentialsFile types.String `tfsdk:"credentials_file"`
	// WarnOnUnusedFacets controls whether the dataplex emits warnings when the user
	// defines facets in their config that the active consumer does not support.
	// Defaults to true. Set to false when migrating from another consumer to suppress
	// noise while you incrementally clean up the config.
	WarnOnUnusedFacets types.Bool `tfsdk:"warn_on_unused_facets"`
}

func (p *OpenLineageProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "openlineage"
	resp.Version = p.version
}

func (p *OpenLineageProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Terraform dataplex for emitting OpenLineage events to GCP Lineage API",
		Attributes: map[string]schema.Attribute{
			"project_id": schema.StringAttribute{
				Description: "GCP Project ID for lineage emissions",
				Required:    true,
			},
			"region": schema.StringAttribute{
				Description: "GCP Region for lineage API",
				Required:    true,
			},
			"credentials_file": schema.StringAttribute{
				Description: "Path to GCP credentials JSON file. If not set, will use Application Default Credentials",
				Optional:    true,
			},
			// warn_on_unused_facets is optional and defaults to true.
			// The dataplex emits a Terraform warning (not an error) whenever the user
			// defines a facet block that the active consumer will ignore — e.g. a
			// 'catalog' block when using the Dataplex consumer.
			// Set to false during migrations from another consumer to suppress these
			// warnings while you clean up unused config incrementally.
			"warn_on_unused_facets": schema.BoolAttribute{
				Optional:    true,
				Description: "Emit warnings when config defines facets the active consumer does not support. Defaults to true. Set to false when migrating from another consumer.",
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

	// Configuration values are now available.
	if config.ProjectID.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("project_id"),
			"Unknown GCP Project ID",
			"The dataplex cannot create the OpenLineage client as there is an unknown configuration value for the GCP project ID.",
		)
	}

	if config.Region.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("region"),
			"Unknown GCP Region",
			"The dataplex cannot create the OpenLineage client as there is an unknown configuration value for the GCP region.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Default values to environment variables, but override
	// with Terraform configuration value if set.
	projectID := os.Getenv("GCP_PROJECT_ID")
	region := os.Getenv("GCP_REGION")
	credentialsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	if !config.ProjectID.IsNull() {
		projectID = config.ProjectID.ValueString()
	}

	if !config.Region.IsNull() {
		region = config.Region.ValueString()
	}

	if !config.CredentialsFile.IsNull() {
		credentialsFile = config.CredentialsFile.ValueString()
	}

	// If any of the expected configurations are missing, return
	// errors with dataplex-specific guidance.
	if projectID == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("project_id"),
			"Missing GCP Project ID",
			"The dataplex cannot create the OpenLineage client as there is a missing or empty value for the GCP project ID.",
		)
	}

	if region == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("region"),
			"Missing GCP Region",
			"The dataplex cannot create the OpenLineage client as there is a missing or empty value for the GCP region.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// warn_on_unused_facets defaults to true if not explicitly set.
	// IsNull() means the user didn't include it in the dataplex block at all.
	warnOnUnusedFacets := true
	if !config.WarnOnUnusedFacets.IsNull() && !config.WarnOnUnusedFacets.IsUnknown() {
		warnOnUnusedFacets = config.WarnOnUnusedFacets.ValueBool()
	}

	// Create configuration for resource initialization
	cfg := &ProviderConfig{
		ProjectID:          projectID,
		Region:             region,
		CredentialsFile:    credentialsFile,
		WarnOnUnusedFacets: warnOnUnusedFacets,
	}

	// Make the configuration available during DataSource and Resource
	// type Configure methods.
	resp.DataSourceData = cfg
	resp.ResourceData = cfg
}

func (p *OpenLineageProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewDataplexJobResource,
	}
}

func (p *OpenLineageProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		// No datasources in simplified architecture
	}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &OpenLineageProvider{
			version: version,
		}
	}
}

// ProviderConfig holds the configuration passed from the dataplex block to every resource.
type ProviderConfig struct {
	ProjectID       string
	Region          string
	CredentialsFile string
	// WarnOnUnusedFacets mirrors the dataplex-level warn_on_unused_facets attribute.
	// Resources read this in Configure() and store it on their struct so Create/Update
	// can decide whether to call warnUnusedFacets().
	WarnOnUnusedFacets bool
}
