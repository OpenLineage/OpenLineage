/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
)

// ResourceBackend is the consumer-specific part shared by job and dataset resources.
// JobResourceBackend and DatasetResourceBackend add a Capability() method returning the
// matching capability type. Consumers implement one of those interfaces and pass it to
// NewJobResource or NewDatasetResource; the base resource owns the schema and the CRUD flow.
type ResourceBackend interface {
	// ConsumerConfigure initialises the consumer client from provider config.
	ConsumerConfigure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse)

	// ConsumerAttributes returns schema attributes added by this consumer
	// (e.g. process_name, run_name) merged into the top-level schema.
	ConsumerAttributes() map[string]schema.Attribute

	// ConsumerBlocks returns schema blocks added by this consumer.
	ConsumerBlocks() map[string]schema.Block

	// NewModel returns a pointer to a fresh zeroed state struct for this consumer. It must
	// have a tfsdk-tagged field for every schema attribute and block, including the
	// consumer's own (usually by embedding JobResourceModel or DatasetResourceModel).
	NewModel() any

	// ConsumerEmit builds and sends the OL event, updating consumer state in model.
	ConsumerEmit(ctx context.Context, model any) diag.Diagnostics

	// ConsumerRead checks whether the entity exists and refreshes computed fields.
	// Returns false if it no longer exists (triggers re-create).
	ConsumerRead(ctx context.Context, model any) (exists bool, diags diag.Diagnostics)

	// ConsumerDelete removes the entity from the consumer.
	ConsumerDelete(ctx context.Context, model any) diag.Diagnostics
}

// resourceBase implements Configure and the CRUD methods for both resource types, delegating
// the consumer-specific parts to the backend. NewJobResource / NewDatasetResource guarantee
// that backend is non-nil.
type resourceBase[B ResourceBackend] struct {
	backend B
}

// schemaWithConsumer merges the consumer's attributes and blocks into the base schema.
func (r *resourceBase[B]) schemaWithConsumer(base schema.Schema) schema.Schema {
	for k, v := range r.backend.ConsumerAttributes() {
		base.Attributes[k] = v
	}
	for k, v := range r.backend.ConsumerBlocks() {
		base.Blocks[k] = v
	}
	return base
}

func (r *resourceBase[B]) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.backend.ConsumerConfigure(ctx, req, resp)
}

// Create reads the plan into a fresh model, calls ConsumerEmit to build and send
// the OL event, then persists the (possibly mutated) model to state.
func (r *resourceBase[B]) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	model := r.backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.backend.ConsumerEmit(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Read refreshes state from the consumer. If ConsumerRead reports the entity no
// longer exists, RemoveResource is called so Terraform plans a re-create.
func (r *resourceBase[B]) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	model := r.backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	exists, diags := r.backend.ConsumerRead(ctx, model)
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

// Update reads the new plan into a fresh model and re-emits the OL event.
// Create and Update intentionally share the same ConsumerEmit path.
func (r *resourceBase[B]) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	model := r.backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.backend.ConsumerEmit(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Delete reads current state and calls ConsumerDelete. On success, RemoveResource
// removes the resource from state entirely.
func (r *resourceBase[B]) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	model := r.backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.backend.ConsumerDelete(ctx, model)...)
	if !resp.Diagnostics.HasError() {
		resp.State.RemoveResource(ctx)
	}
}
