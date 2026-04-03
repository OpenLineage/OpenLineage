/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
)

// DatasetResourceBackend defines the consumer-specific operations for dataset resources.
type DatasetResourceBackend interface {
	Capability() DatasetCapability
	ConsumerConfigure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse)
	ConsumerAttributes() map[string]schema.Attribute
	ConsumerBlocks() map[string]schema.Block
	NewModel() any
	ConsumerEmit(ctx context.Context, model any, runID uuid.UUID) diag.Diagnostics
	ConsumerRead(ctx context.Context, model any) (exists bool, diags diag.Diagnostics)
	ConsumerDelete(ctx context.Context, model any) diag.Diagnostics
}

// BaseDatasetResource is the generic base for all dataset resources.
// Owns Metadata, Schema, and the full CRUD flow.
type BaseDatasetResource struct {
	Backend DatasetResourceBackend
}

func (r *BaseDatasetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

func (r *BaseDatasetResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	s := GenerateDatasetSchema(r.Backend.Capability())
	for k, v := range r.Backend.ConsumerAttributes() {
		s.Attributes[k] = v
	}
	for k, v := range r.Backend.ConsumerBlocks() {
		s.Blocks[k] = v
	}
	resp.Schema = s
}

func (r *BaseDatasetResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.Backend.ConsumerConfigure(ctx, req, resp)
}

func (r *BaseDatasetResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerEmit(ctx, model, uuid.New())...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func (r *BaseDatasetResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	exists, diags := r.Backend.ConsumerRead(ctx, model)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	if !exists {
		resp.State.RemoveResource(ctx)
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func (r *BaseDatasetResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerEmit(ctx, model, uuid.New())...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

func (r *BaseDatasetResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerDelete(ctx, model)...)
}
