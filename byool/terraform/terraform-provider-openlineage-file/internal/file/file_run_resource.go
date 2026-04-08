/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package file

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"

	ol "github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
)

var _ ol.JobResourceBackend = &FileRunResource{}
var _ resource.Resource = &FileRunResource{}

func NewFileRunResource() resource.Resource {
	r := &FileRunResource{}
	r.Backend = r
	return r
}

// FileRunResource emits an OpenLineage RunEvent — a job execution with a
// unique run ID, inputs, and outputs. This is the richest event type: it
// captures both what the job IS (facets) and what happened during a specific
// execution (run ID, inputs, outputs with column lineage).
type FileRunResource struct {
	ol.BaseJobResource
	outputDir   string
	prettyPrint bool
}

// Metadata overrides BaseJobResource to register this resource as openlineage_run
// rather than the default openlineage_job.
func (r *FileRunResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_run"
}

func (r *FileRunResource) Capability() ol.JobCapability {
	return ol.EmptyJobCapability().WithFacetEnabled(
		ol.FacetJobType,
		ol.FacetJobOwnership,
		ol.FacetDatasetSymlinks,
		ol.FacetDatasetSchema,
		ol.FacetDatasetColumnLineage,
	)
}

func (r *FileRunResource) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	config, ok := req.ProviderData.(*ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError("Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *ProviderConfig, got: %T.", req.ProviderData))
		return
	}
	r.outputDir = config.OutputDir
	r.prettyPrint = config.PrettyPrint
}

func (r *FileRunResource) ConsumerAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"file_path": schema.StringAttribute{
			Computed:    true,
			Description: "Absolute path to the JSON file written on disk.",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.UseStateForUnknown(),
			},
		},
		"last_emitted": schema.StringAttribute{
			Computed:    true,
			Description: "RFC3339 UTC timestamp of the most recent write to disk. Updated on every apply.",
		},
		"emit_count": schema.Int64Attribute{
			Computed:    true,
			Description: "Number of times this run's lineage has been emitted. Starts at 1, increments on each apply.",
		},
	}
}

func (r *FileRunResource) ConsumerBlocks() map[string]schema.Block {
	return map[string]schema.Block{}
}

func (r *FileRunResource) NewModel() any {
	return &FileRunModel{}
}

// ConsumerEmit builds an OpenLineage RunEvent (includes run ID, inputs, outputs)
// and writes it to disk as {output_dir}/run__{namespace}__{name}.json.
func (r *FileRunResource) ConsumerEmit(ctx context.Context, modelAny any, _ uuid.UUID) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileRunModel)

	namespace := model.Namespace.ValueString()
	name := model.Name.ValueString()

	tflog.Info(ctx, "Emitting OL RunEvent to JSON file", map[string]any{
		"namespace": namespace,
		"name":      name,
	})

	path := FilePath(r.outputDir, "run", namespace, name)

	var prevCount int64
	if existing, err := Read(path); err != nil {
		tflog.Warn(ctx, "Could not read existing run file; emit_count will reset to 1", map[string]any{
			"file_path": path, "error": err.Error(),
		})
	} else if existing != nil {
		prevCount = existing.EmitCount
	}

	event := ol.BuildRunEvent(&model.JobResourceModel)
	eventBytes, err := json.Marshal(event.AsEmittable())
	if err != nil {
		diags.AddError("Event Serialisation Error",
			fmt.Sprintf("Unable to serialise OpenLineage RunEvent: %s", err))
		return diags
	}

	newCount := prevCount + 1
	timestamp := now()

	rec := &FileRecord{
		SchemaVersion: "1.0",
		Entity:        EntityRef{Namespace: namespace, Name: name},
		LastEmitted:   timestamp,
		EmitCount:     newCount,
		RunID:         event.Run.RunID,
		Event:         json.RawMessage(eventBytes),
	}

	if err := Write(path, rec, r.prettyPrint); err != nil {
		diags.AddError("Write Error",
			fmt.Sprintf("Unable to write run file %s: %s", path, err))
		return diags
	}

	model.FilePath = types.StringValue(path)
	model.LastEmitted = types.StringValue(timestamp)
	model.EmitCount = types.Int64Value(newCount)

	tflog.Info(ctx, "Run file written", map[string]any{
		"file_path": path, "emit_count": newCount, "run_id": event.Run.RunID,
	})
	return diags
}

func (r *FileRunResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	model := modelAny.(*FileRunModel)

	path := model.FilePath.ValueString()
	if path == "" {
		return true, diags
	}

	rec, err := Read(path)
	if err != nil {
		diags.AddError("Read Error", fmt.Sprintf("Unable to read run file %s: %s", path, err))
		return false, diags
	}
	if rec == nil {
		tflog.Warn(ctx, "Run file not found — drift detected, will re-create", map[string]any{"file_path": path})
		return false, diags
	}

	model.EmitCount = types.Int64Value(rec.EmitCount)
	model.LastEmitted = types.StringValue(rec.LastEmitted)
	return true, diags
}

func (r *FileRunResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileRunModel)

	path := model.FilePath.ValueString()
	if path == "" {
		return diags
	}

	tflog.Info(ctx, "Deleting run file", map[string]any{"file_path": path})
	if err := Delete(path); err != nil {
		diags.AddError("Delete Error", fmt.Sprintf("Unable to delete run file %s: %s", path, err))
	}
	return diags
}
