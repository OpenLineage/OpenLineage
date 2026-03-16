/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package dataplex

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/tomasznazarewicz/openlineage-terraform-resources/ol"
)

// Compile-time checks.
var _ ol.JobResourceBackend = &DataplexJobResource{}
var _ resource.Resource = &DataplexJobResource{}

// NewDataplexJobResource is the constructor registered in dataplex.go.
// It wires DataplexJobResource as both the resource.Resource and its own backend —
// the concrete struct IS the backend implementation.
func NewDataplexJobResource() resource.Resource {
	r := &DataplexJobResource{}
	r.Backend = r // self-reference: DataplexJobResource implements JobResourceBackend
	return r
}

// DataplexJobResource is the Dataplex-specific concrete job resource.
//
// It embeds ol.BaseJobResource (the generic CRUD logic) and implements
// ol.JobResourceBackend (the consumer-specific operations).
//
// Analogous to:
//
//	class DataplexJobResource extends BaseJobResource {
//	    void emit()      { ... gRPC call ... }
//	    void readState() { ... GetProcess ... }
//	    void delete()    { ... DeleteProcess ... }
//	}
//
// The generic CRUD methods (Create, Read, Update, Delete, Schema, Metadata)
// are all inherited from BaseJobResource — this struct does not override them.
type DataplexJobResource struct {
	ol.BaseJobResource                 // inherits Metadata, Schema, Create, Read, Update, Delete
	dpClient           *dataplexClient // Dataplex gRPC client — created in Configure()
	projectID          string
	region             string
}

// ── ol.JobResourceBackend implementation ─────────────────────────────────────

// Capability declares which OL facets the Dataplex consumer supports.
// Dataplex accepts RunEvents but only uses a subset of facets to build
// the Process entity — the rest are silently ignored by the API.
func (r *DataplexJobResource) Capability() ol.JobCapability {
	return ol.FullJobCapability().WithFacetDisabled(
		// Job facets Dataplex does not use
		ol.FacetJobDocumentation,
		ol.FacetJobSourceCode,
		ol.FacetJobSourceCodeLocation,
		ol.FacetJobSQL,
		ol.FacetJobTags,
		// Dataset facets Dataplex does not use
		ol.FacetDatasetSchema,
		ol.FacetDatasetDataSource,
		ol.FacetDatasetDocumentation,
		ol.FacetDatasetType,
		ol.FacetDatasetVersion,
		ol.FacetDatasetStorage,
		ol.FacetDatasetOwnership,
		ol.FacetDatasetLifecycleStateChange,
		ol.FacetDatasetHierarchy,
		ol.FacetDatasetTags,
	)
}

// ConsumerConfigure creates the Dataplex gRPC client from dataplex config.
// Called once by Terraform after the dataplex block is parsed.
func (r *DataplexJobResource) ConsumerConfigure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	config, ok := req.ProviderData.(*ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError("Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *ProviderConfig, got: %T", req.ProviderData))
		return
	}

	r.projectID = config.ProjectID
	r.region = config.Region

	dpClient, err := newDataplexClient(ctx, config.ProjectID, config.Region, config.CredentialsFile)
	if err != nil {
		resp.Diagnostics.AddError("Dataplex Client Error",
			fmt.Sprintf("Unable to create Dataplex client: %s", err))
		return
	}
	r.dpClient = dpClient
}

func (r *DataplexJobResource) ConsumerAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"process_name": schema.StringAttribute{
			Computed:    true,
			Description: "Dataplex process resource name",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.UseStateForUnknown(),
			},
		},
		"run_id":             schema.StringAttribute{Computed: true, Description: "UUID of the latest emitted run"},
		"run_name":           schema.StringAttribute{Computed: true, Description: "Dataplex run resource name"},
		"lineage_event_name": schema.StringAttribute{Computed: true, Description: "Dataplex lineage event resource name"},
		"update_time":        schema.StringAttribute{Computed: true, Description: "Process last update time (ISO 8601)"},
	}
}

func (r *DataplexJobResource) ConsumerBlocks() map[string]schema.Block {
	return map[string]schema.Block{}
}

func (r *DataplexJobResource) NewModel() any {
	return &DataplexJobModel{}
}

func (r *DataplexJobResource) ConsumerEmit(ctx context.Context, modelAny any, runID uuid.UUID) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*DataplexJobModel)

	model.ID = types.StringValue(
		fmt.Sprintf("%s.%s", model.Namespace.ValueString(), model.Name.ValueString()),
	)
	model.RunID = types.StringValue(runID.String())

	tflog.Info(ctx, "Emitting OL event to Dataplex", map[string]any{
		"namespace": model.Namespace.ValueString(),
		"name":      model.Name.ValueString(),
		"run_id":    runID.String(),
	})

	event := ol.BuildRunEvent(&model.JobResourceModel, runID)
	result, err := r.dpClient.emitAndCapture(ctx, event.AsEmittable())
	if err != nil {
		diags.AddError("Emission Error", fmt.Sprintf("Unable to emit event: %s", err))
		return diags
	}

	model.ProcessName = types.StringValue(result.ProcessName)
	model.RunName = types.StringValue(result.RunName)
	if len(result.LineageEventNames) > 0 {
		model.LineageEventName = types.StringValue(result.LineageEventNames[0])
	}

	tflog.Info(ctx, "Event emitted", map[string]any{
		"process_name": result.ProcessName,
		"run_name":     result.RunName,
	})

	r.refreshRunState(ctx, model)
	return diags
}

func (r *DataplexJobResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	model := modelAny.(*DataplexJobModel)

	processName := model.ProcessName.ValueString()
	if processName == "" {
		return true, diags
	}

	process, err := r.dpClient.getProcess(ctx, processName)
	if err != nil {
		diags.AddError("Read Error", fmt.Sprintf("Unable to read Dataplex process: %s", err))
		return false, diags
	}
	if process == nil {
		tflog.Warn(ctx, "Dataplex process no longer exists — drift detected", map[string]any{
			"process_name": processName,
		})
		return false, diags
	}
	if !process.OriginVerified {
		tflog.Warn(ctx, "Dataplex process origin does not match dataplex", map[string]any{
			"process_name": processName,
			"origin_name":  process.OriginName,
			"expected":     ol.ProviderOriginName,
		})
	}

	r.refreshRunState(ctx, model)
	return true, diags
}

func (r *DataplexJobResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*DataplexJobModel)

	processName := model.ProcessName.ValueString()
	if processName == "" || r.dpClient == nil {
		return diags
	}

	tflog.Info(ctx, "Deleting Dataplex process", map[string]any{"process_name": processName})
	if err := r.dpClient.deleteProcess(ctx, processName); err != nil {
		diags.AddError("Delete Error",
			fmt.Sprintf("Unable to delete Dataplex process %s: %s", processName, err))
	}
	return diags
}

func (r *DataplexJobResource) refreshRunState(ctx context.Context, data *DataplexJobModel) {
	if r.dpClient == nil || data.ProcessName.ValueString() == "" {
		return
	}
	run, err := r.dpClient.getLatestRun(ctx, data.ProcessName.ValueString())
	if err != nil {
		tflog.Warn(ctx, "Failed to refresh run state", map[string]any{
			"error":        err.Error(),
			"process_name": data.ProcessName.ValueString(),
		})
		return
	}
	if run == nil {
		return
	}
	if !run.EndTime.IsZero() {
		data.UpdateTime = types.StringValue(run.EndTime.Format(time.RFC3339))
	} else if !run.StartTime.IsZero() {
		data.UpdateTime = types.StringValue(run.StartTime.Format(time.RFC3339))
	}
	tflog.Info(ctx, "Run state refreshed", map[string]any{
		"process_name": data.ProcessName.ValueString(),
	})
}
