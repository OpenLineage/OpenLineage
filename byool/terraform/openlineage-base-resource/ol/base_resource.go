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

const backendNilSummary = "Backend not initialised"
const backendNilDetail = "Backend is nil. " +
	"Make sure the concrete resource sets r.Backend = r in its constructor."

// ResourceBackend is the common contract shared by all resource backends.
// JobResourceBackend and DatasetResourceBackend embed this interface and each
// add their own Capability() method returning the appropriate capability type.
//
// The unexported baseSchema() method is satisfied through promotion: types
// that embed BaseJobResource or BaseDatasetResource get the right implementation
// automatically. This enforces the expected embedding pattern and lets
// resourceBase.Schema be defined once for both resource types.
type ResourceBackend interface {
	// ConsumerConfigure initialises the consumer client from provider config.
	ConsumerConfigure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse)

	// ConsumerAttributes returns schema attributes added by this consumer
	// (e.g. process_name, run_name) merged into the top-level schema.
	ConsumerAttributes() map[string]schema.Attribute

	// ConsumerBlocks returns schema blocks added by this consumer.
	ConsumerBlocks() map[string]schema.Block

	// NewModel returns a fresh zeroed state struct for this consumer.
	NewModel() any

	// ConsumerEmit builds and sends the OL event, updating consumer state in model.
	ConsumerEmit(ctx context.Context, model any) diag.Diagnostics

	// ConsumerRead checks whether the entity exists and refreshes computed fields.
	// Returns false if it no longer exists (triggers re-create).
	ConsumerRead(ctx context.Context, model any) (exists bool, diags diag.Diagnostics)

	// ConsumerDelete removes the entity from the consumer.
	ConsumerDelete(ctx context.Context, model any) diag.Diagnostics

	// BaseSchema returns the capability-driven base schema for this resource type.
	// Implemented by BaseJobResource (GenerateJobSchema) and BaseDatasetResource
	// (GenerateDatasetSchema) — consumers inherit it through embedding.
	BaseSchema() schema.Schema
}

// resourceBase is embedded in BaseJobResource and BaseDatasetResource.
// It holds the typed Backend field and the shared nil-check helper,
// eliminating the need to duplicate them in every base resource type.
//
// B is constrained to ResourceBackend so both JobResourceBackend and
// DatasetResourceBackend are valid type arguments.
type resourceBase[B ResourceBackend] struct {
	Backend B
}

// checkBackend adds a diagnostic error and returns false when Backend is nil.
// Each CRUD method calls this at the top: if !r.checkBackend(&resp.Diagnostics) { return }
//
// Note on nil semantics: any(r.Backend) correctly reflects nil-ness for interface
// type arguments — a nil B (interface) converts to a nil any, unlike a nil pointer
// which would produce a non-nil any.
func (r *resourceBase[B]) checkBackend(diags *diag.Diagnostics) bool {
	if any(r.Backend) != nil {
		return true
	}
	diags.AddError(backendNilSummary, backendNilDetail)
	return false
}

// mergeConsumerSchema merges consumer-specific attributes and blocks into base,
// then writes the result to out. Must be called after checkBackend has passed.
func (r *resourceBase[B]) mergeConsumerSchema(out *schema.Schema, base schema.Schema) {
	for k, v := range r.Backend.ConsumerAttributes() {
		base.Attributes[k] = v
	}
	for k, v := range r.Backend.ConsumerBlocks() {
		base.Blocks[k] = v
	}
	*out = base
}

// Schema calls Backend.baseSchema() to obtain the capability-driven base schema,
// then merges in any consumer-specific attributes and blocks.
// baseSchema() is implemented by BaseJobResource and BaseDatasetResource and
// promoted to consumer types through embedding.
func (r *resourceBase[B]) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
	r.mergeConsumerSchema(&resp.Schema, r.Backend.BaseSchema())
}

func (r *resourceBase[B]) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
	r.Backend.ConsumerConfigure(ctx, req, resp)
}

// Create reads the plan into a fresh model, calls ConsumerEmit to build and send
// the OL event, then persists the (possibly mutated) model to state.
func (r *resourceBase[B]) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerEmit(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Read refreshes state from the consumer. If ConsumerRead reports the entity no
// longer exists, RemoveResource is called so Terraform plans a re-create.
func (r *resourceBase[B]) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
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

// Update reads the new plan into a fresh model and re-emits the OL event.
// Create and Update intentionally share the same ConsumerEmit path — both
// result in a COMPLETE event being sent to the consumer.
func (r *resourceBase[B]) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.Plan.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerEmit(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, model)...)
}

// Delete reads current state and calls ConsumerDelete. On success, RemoveResource
// removes the resource from state entirely.
func (r *resourceBase[B]) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	if !r.checkBackend(&resp.Diagnostics) {
		return
	}
	model := r.Backend.NewModel()
	resp.Diagnostics.Append(req.State.Get(ctx, model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(r.Backend.ConsumerDelete(ctx, model)...)
	if !resp.Diagnostics.HasError() {
		resp.State.RemoveResource(ctx)
	}
}
