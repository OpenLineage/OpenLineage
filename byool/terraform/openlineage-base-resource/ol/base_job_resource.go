/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
)

// JobResourceBackend defines the consumer-specific operations for job resources.
type JobResourceBackend interface {
	ResourceBackend
	// Capability declares which OL facets this consumer supports.
	Capability() JobCapability
}

var _ resource.ResourceWithConfigure = (*jobResource)(nil)

// jobResource is the "<provider>_job" resource. The schema comes from the backend's
// capability plus its consumer attributes and blocks.
type jobResource struct {
	resourceBase[JobResourceBackend]
}

// NewJobResource returns the "<provider>_job" resource backed by b. Use it in the
// provider's Resources list: func() resource.Resource { return ol.NewJobResource(newMyBackend()) }.
// It panics if b is nil.
func NewJobResource(b JobResourceBackend) resource.Resource {
	if b == nil {
		panic("ol.NewJobResource: backend is nil")
	}
	return &jobResource{resourceBase[JobResourceBackend]{backend: b}}
}

func (r *jobResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_job"
}

func (r *jobResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = r.schemaWithConsumer(GenerateJobSchema(r.backend.Capability()))
}
