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

// JobResourceBackend defines the consumer-specific operations that BaseJobResource
// delegates to. Each consumer (Dataplex, Marquez, …) implements this interface.
//
// Java analogy:
//
//	interface JobResourceBackend {
//	    void consumerEmit();
//	    void consumerRead();
//	    void consumerDelete();
//	}
type JobResourceBackend interface {
	// Capability declares which OL facets this consumer supports.
	Capability() JobCapability

	// ConsumerConfigure initialises the consumer client from dataplex config.
	ConsumerConfigure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse)

	// ConsumerAttributes returns schema attributes added by this consumer
	// (e.g. process_name, run_name) merged into the top-level schema.
	ConsumerAttributes() map[string]schema.Attribute

	// ConsumerBlocks returns schema blocks added by this consumer.
	ConsumerBlocks() map[string]schema.Block

	// NewModel returns a fresh zeroed state struct for this consumer.
	NewModel() any

	// ConsumerEmit builds and sends the OL event, updating consumer state in model.
	ConsumerEmit(ctx context.Context, model any, runID uuid.UUID) diag.Diagnostics

	// ConsumerRead checks whether the entity exists and refreshes computed fields.
	// Returns false if it no longer exists (triggers re-create).
	ConsumerRead(ctx context.Context, model any) (exists bool, diags diag.Diagnostics)

	// ConsumerDelete removes the entity from the consumer.
	ConsumerDelete(ctx context.Context, model any) diag.Diagnostics
}

// BaseJobResource is the generic base for all job resources.
// Owns Metadata, Schema, and the full CRUD flow.
// Consumer-specific behaviour is entirely delegated to JobResourceBackend.
//
// Usage:
//
//	type DataplexJobResource struct {
//	    ol.BaseJobResource
//	    dpClient *dataplexClient
//	}
//
//	func NewDataplexJobResource() resource.Resource {
//	    r := &DataplexJobResource{}
//	    r.Backend = r  // self-reference: concrete struct IS the backend
//	    return r
//	}
type BaseJobResource struct {
	Backend JobResourceBackend
}

func (r *BaseJobResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_job"
}

// Schema generates the schema from the backend's JobCapability, then merges
// in any consumer-specific attributes and blocks.
func (r *BaseJobResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	s := GenerateJobSchema(r.Backend.Capability())
	for k, v := range r.Backend.ConsumerAttributes() {
		s.Attributes[k] = v
	}
	for k, v := range r.Backend.ConsumerBlocks() {
		s.Blocks[k] = v
	}
	resp.Schema = s
}

func (r *BaseJobResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.Backend.ConsumerConfigure(ctx, req, resp)
}

func (r *BaseJobResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
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

func (r *BaseJobResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
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

func (r *BaseJobResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
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

func (r *BaseJobResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerDelete(ctx, model)...)
}
