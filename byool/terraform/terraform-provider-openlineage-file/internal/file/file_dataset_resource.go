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

	"github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
)

var _ ol.DatasetResourceBackend = &FileDatasetResource{}
var _ resource.Resource = &FileDatasetResource{}

func NewFileDatasetResource() resource.Resource {
	r := &FileDatasetResource{}
	r.Backend = r
	return r
}

// FileDatasetResource emits an OpenLineage DatasetEvent — standalone dataset
// metadata independent of any job execution.
// Written to: {output_dir}/dataset__{namespace}__{name}.json
type FileDatasetResource struct {
	ol.BaseDatasetResource
	outputDir   string
	prettyPrint bool
}

func (r *FileDatasetResource) Capability() ol.DatasetCapability {
	return ol.EmptyDatasetCapability().WithFacetEnabled(
		ol.FacetDatasetSymlinks,
		ol.FacetDatasetSchema,
		ol.FacetDatasetDocumentation,
		ol.FacetDatasetOwnership,
	)
}

func (r *FileDatasetResource) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *FileDatasetResource) ConsumerAttributes() map[string]schema.Attribute {
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
			Description: "Number of times this dataset's lineage has been emitted. Starts at 1, increments on each apply.",
		},
	}
}

func (r *FileDatasetResource) ConsumerBlocks() map[string]schema.Block {
	return map[string]schema.Block{}
}

func (r *FileDatasetResource) NewModel() any {
	return &FileDatasetModel{}
}

// ConsumerEmit builds an OpenLineage DatasetEvent and writes it to
// {output_dir}/dataset__{namespace}__{name}.json.
func (r *FileDatasetResource) ConsumerEmit(ctx context.Context, modelAny any, _ uuid.UUID) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileDatasetModel)

	namespace := model.Namespace.ValueString()
	name := model.Name.ValueString()

	tflog.Info(ctx, "Emitting OL DatasetEvent to JSON file", map[string]any{
		"namespace": namespace, "name": name,
	})

	path := FilePath(r.outputDir, "dataset", namespace, name)

	var prevCount int64
	if existing, err := Read(path); err != nil {
		tflog.Warn(ctx, "Could not read existing dataset file; emit_count will reset to 1", map[string]any{
			"file_path": path, "error": err.Error(),
		})
	} else if existing != nil {
		prevCount = existing.EmitCount
	}

	event := ol.BuildDatasetEvent(&model.DatasetResourceModel)
	eventBytes, err := json.Marshal(event.AsEmittable())
	if err != nil {
		diags.AddError("Event Serialisation Error",
			fmt.Sprintf("Unable to serialise OpenLineage DatasetEvent: %s", err))
		return diags
	}

	newCount := prevCount + 1
	timestamp := now()

	rec := &FileRecord{
		SchemaVersion: "1.0",
		Entity:        EntityRef{Namespace: namespace, Name: name},
		LastEmitted:   timestamp,
		EmitCount:     newCount,
		Event:         json.RawMessage(eventBytes),
	}

	if err := Write(path, rec, r.prettyPrint); err != nil {
		diags.AddError("Write Error", fmt.Sprintf("Unable to write dataset file %s: %s", path, err))
		return diags
	}

	model.FilePath = types.StringValue(path)
	model.LastEmitted = types.StringValue(timestamp)
	model.EmitCount = types.Int64Value(newCount)

	tflog.Info(ctx, "Dataset file written", map[string]any{
		"file_path": path, "emit_count": newCount,
	})
	return diags
}

func (r *FileDatasetResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	model := modelAny.(*FileDatasetModel)

	path := model.FilePath.ValueString()
	if path == "" {
		return true, diags
	}

	rec, err := Read(path)
	if err != nil {
		diags.AddError("Read Error", fmt.Sprintf("Unable to read dataset file %s: %s", path, err))
		return false, diags
	}
	if rec == nil {
		tflog.Warn(ctx, "Dataset file not found — drift detected, will re-create", map[string]any{"file_path": path})
		return false, diags
	}

	model.EmitCount = types.Int64Value(rec.EmitCount)
	model.LastEmitted = types.StringValue(rec.LastEmitted)
	return true, diags
}

func (r *FileDatasetResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileDatasetModel)

	path := model.FilePath.ValueString()
	if path == "" {
		return diags
	}

	tflog.Info(ctx, "Deleting dataset file", map[string]any{"file_path": path})
	if err := Delete(path); err != nil {
		diags.AddError("Delete Error", fmt.Sprintf("Unable to delete dataset file %s: %s", path, err))
	}
	return diags
}
