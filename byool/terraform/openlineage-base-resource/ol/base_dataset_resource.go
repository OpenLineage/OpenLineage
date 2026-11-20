/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
)

// DatasetResourceBackend defines the consumer-specific operations for dataset resources.
type DatasetResourceBackend interface {
	ResourceBackend
	// Capability declares which dataset facets this consumer supports.
	Capability() DatasetCapability
}

var _ resource.ResourceWithConfigure = (*datasetResource)(nil)

// datasetResource is the "<provider>_dataset" resource. The schema comes from the backend's
// capability plus its consumer attributes and blocks.
type datasetResource struct {
	resourceBase[DatasetResourceBackend]
}

// NewDatasetResource returns the "<provider>_dataset" resource backed by b. Use it in the
// provider's Resources list: func() resource.Resource { return ol.NewDatasetResource(newMyBackend()) }.
// It panics if b is nil.
func NewDatasetResource(b DatasetResourceBackend) resource.Resource {
	if b == nil {
		panic("ol.NewDatasetResource: backend is nil")
	}
	return &datasetResource{resourceBase[DatasetResourceBackend]{backend: b}}
}

func (r *datasetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

func (r *datasetResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = r.schemaWithConsumer(GenerateDatasetSchema(r.backend.Capability()))
}
