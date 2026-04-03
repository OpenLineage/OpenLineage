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

// Compile-time interface checks.
// If JsonFileJobResource stops satisfying either interface the build fails
// with a clear message — catch mistakes before they surface at runtime.
var _ ol.JobResourceBackend = &FileJobResource{}
var _ resource.Resource = &FileJobResource{}

// NewFileJobResource is the constructor registered in provider.go.
// It wires JsonFileJobResource as both resource.Resource and its own backend —
// the struct IS the backend implementation (self-reference pattern).
func NewFileJobResource() resource.Resource {
	r := &FileJobResource{}
	r.Backend = r // self-reference: this struct implements ol.JobResourceBackend
	return r
}

// FileJobResource emits an OpenLineage JobEvent — static job metadata without
// a run ID. Use this to register a job's identity, ownership, and typical
// inputs/outputs independently of any specific execution.
// Written to: {output_dir}/job__{namespace}__{name}.json
type FileJobResource struct {
	ol.BaseJobResource        // inherits Metadata, Schema, Create, Read, Update, Delete
	outputDir          string // resolved from ProviderConfig.OutputDir
	prettyPrint        bool   // resolved from ProviderConfig.PrettyPrint
}

// ── ol.JobResourceBackend implementation ─────────────────────────────────────

// Capability declares which OpenLineage facets this consumer supports.
//
// The JSON file backend enables ALL facets — it stores whatever the user
// configures verbatim in the event JSON.
//
// The additive model (EmptyJobCapability + WithFacetEnabled) makes the
// supported surface explicit: every facet in this list is a deliberate
// choice, not a leftover from a "disable what you don't need" approach.
//
// Compare with Dataplex, which enables only the subset it maps to its
// data model: job_type, ownership, symlinks, catalog, column_lineage.
func (r *FileJobResource) Capability() ol.JobCapability {
	return ol.EmptyJobCapability().WithFacetEnabled(
		// Job facets
		ol.FacetJobType,
		ol.FacetJobOwnership,
		//ol.FacetJobDocumentation,
		//ol.FacetJobSourceCode,
		//ol.FacetJobSourceCodeLocation,
		//ol.FacetJobSQL,
		//ol.FacetJobTags,
		// Dataset facets
		ol.FacetDatasetSymlinks,
		ol.FacetDatasetSchema,
		//ol.FacetDatasetDataSource,
		//ol.FacetDatasetDocumentation,
		//ol.FacetDatasetType,
		//ol.FacetDatasetVersion,
		//ol.FacetDatasetStorage,
		//ol.FacetDatasetOwnership,
		//ol.FacetDatasetLifecycleStateChange,
		//ol.FacetDatasetHierarchy,
		//ol.FacetDatasetCatalog,
		ol.FacetDatasetColumnLineage,
		//ol.FacetDatasetTags,
	)
}

// ConsumerConfigure extracts provider-level config and stores it on the resource.
// Called once by Terraform after the provider block is parsed, before any CRUD.
//
// ProviderConfig is passed via req.ProviderData — it was placed there by
// OpenLineageProvider.Configure(). The type assertion makes the coupling
// explicit and produces a clear error if the types ever diverge.
func (r *FileJobResource) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		// Provider not yet configured. This is normal during planning for
		// resources that have not been created yet — not an error.
		return
	}

	config, ok := req.ProviderData.(*ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf(
				"Expected *ProviderConfig, got: %T. "+
					"This is a bug in the provider — please report it.",
				req.ProviderData,
			),
		)
		return
	}

	r.outputDir = config.OutputDir
	r.prettyPrint = config.PrettyPrint
}

// ConsumerAttributes declares the computed attributes that this backend adds
// to the Terraform schema alongside the generic OL attributes.
//
// These are the "receipts" the backend writes back to state after each emit.
// Compare with Dataplex's consumer attributes: process_name, run_name,
// lineage_event_name, update_time — all returned by the GCP API.
// Here we derive equivalent information locally from the file on disk.
func (r *FileJobResource) ConsumerAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		// file_path is stable — same namespace+name always maps to the same file.
		// UseStateForUnknown prevents "(known after apply)" on every subsequent plan,
		// matching the pattern used for Dataplex's process_name.
		"file_path": schema.StringAttribute{
			Computed: true,
			Description: "Absolute path to the JSON file written on disk. " +
				"Derived from output_dir, namespace, and name. " +
				"Stable across applies — does not change after the initial creation.",
			PlanModifiers: []planmodifier.String{
				stringplanmodifier.UseStateForUnknown(),
			},
		},

		// last_emitted changes on every apply — no UseStateForUnknown.
		// Terraform will show it as "(known after apply)" on every plan, which
		// accurately reflects that the timestamp is determined at apply time.
		"last_emitted": schema.StringAttribute{
			Computed:    true,
			Description: "RFC3339 UTC timestamp of the most recent write to disk. Updated on every apply.",
		},

		// emit_count increments on every apply.
		// Like last_emitted, it is intentionally not stabilised with UseStateForUnknown.
		"emit_count": schema.Int64Attribute{
			Computed: true,
			Description: "Number of times this job's lineage has been emitted. " +
				"Starts at 1 on creation, increments on each subsequent apply.",
		},
	}
}

// ConsumerBlocks declares no extra blocks — the JSON file backend has no
// nested configuration beyond what the generic OL schema already provides.
func (r *FileJobResource) ConsumerBlocks() map[string]schema.Block {
	return map[string]schema.Block{}
}

// NewModel returns a fresh, zeroed model for this resource.
// Called by BaseJobResource.Create() and BaseJobResource.Update() before
// deserialising the Terraform plan into the model.
func (r *FileJobResource) NewModel() any {
	return &FileJobModel{}
}

// ConsumerEmit builds an OpenLineage JobEvent (no run ID) and writes it to
// {output_dir}/job__{namespace}__{name}.json.
func (r *FileJobResource) ConsumerEmit(ctx context.Context, modelAny any, _ uuid.UUID) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileJobModel)

	namespace := model.Namespace.ValueString()
	name := model.Name.ValueString()

	tflog.Info(ctx, "Emitting OL JobEvent to JSON file", map[string]any{
		"namespace": namespace, "name": name,
	})

	path := FilePath(r.outputDir, "job", namespace, name)

	var prevCount int64
	if existing, err := Read(path); err != nil {
		tflog.Warn(ctx, "Could not read existing job file; emit_count will reset to 1", map[string]any{
			"file_path": path, "error": err.Error(),
		})
	} else if existing != nil {
		prevCount = existing.EmitCount
	}

	event := ol.BuildJobEvent(&model.JobResourceModel)
	eventBytes, err := json.Marshal(event.AsEmittable())
	if err != nil {
		diags.AddError("Event Serialisation Error",
			fmt.Sprintf("Unable to serialise OpenLineage JobEvent: %s", err))
		return diags
	}

	newCount := prevCount + 1
	timestamp := now()

	rec := &FileRecord{
		SchemaVersion: "1.0",
		Job:           JobRef{Namespace: namespace, Name: name},
		LastEmitted:   timestamp,
		EmitCount:     newCount,
		Event:         json.RawMessage(eventBytes),
	}

	if err := Write(path, rec, r.prettyPrint); err != nil {
		diags.AddError("Write Error", fmt.Sprintf("Unable to write job file %s: %s", path, err))
		return diags
	}

	model.FilePath = types.StringValue(path)
	model.LastEmitted = types.StringValue(timestamp)
	model.EmitCount = types.Int64Value(newCount)

	tflog.Info(ctx, "Job file written", map[string]any{
		"file_path": path, "emit_count": newCount,
	})
	return diags
}

// ConsumerRead checks whether the lineage file still exists and refreshes
// computed state from its contents.
//
// Flow:
//  1. If FilePath is empty (resource was never fully created) → return true (no-op)
//  2. Try to read the file from disk
//  3. If the file is missing → return false → BaseJobResource removes the resource
//     from state and marks it for re-create on the next plan
//  4. If the file exists → refresh EmitCount and LastEmitted from the file
//
// Returning false on a missing file is the exact same contract as Dataplex's
// ConsumerRead returning false when GetProcess returns 404.
func (r *FileJobResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	model := modelAny.(*FileJobModel)

	path := model.FilePath.ValueString()
	if path == "" {
		// Resource was never fully created — nothing to check.
		return true, diags
	}

	rec, err := Read(path)
	if err != nil {
		diags.AddError("Read Error", fmt.Sprintf("Unable to read job file %s: %s", path, err))
		return false, diags
	}
	if rec == nil {
		tflog.Warn(ctx, "Job file not found — drift detected, will re-create", map[string]any{"file_path": path})
		return false, diags
	}

	model.EmitCount = types.Int64Value(rec.EmitCount)
	model.LastEmitted = types.StringValue(rec.LastEmitted)
	return true, diags
}

// ConsumerDelete removes the lineage file from disk.
//
// Idempotent by design: if the file is already gone, Delete returns nil.
// This matches Terraform's expectation that Delete succeeds even if the
// resource was already destroyed out-of-band.
func (r *FileJobResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
	var diags diag.Diagnostics
	model := modelAny.(*FileJobModel)

	path := model.FilePath.ValueString()
	if path == "" {
		return diags
	}

	tflog.Info(ctx, "Deleting job file", map[string]any{"file_path": path})
	if err := Delete(path); err != nil {
		diags.AddError("Delete Error", fmt.Sprintf("Unable to delete job file %s: %s", path, err))
	}
	return diags
}
