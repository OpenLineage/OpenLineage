/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
)

// DatasetResourceBackend defines the consumer-specific operations for dataset resources.
// The 7 shared methods are inherited from ResourceBackend; only Capability() is added here.
type DatasetResourceBackend interface {
	ResourceBackend
	// Capability declares which dataset facets this consumer supports.
	Capability() DatasetCapability
}

// BaseDatasetResource is the generic base for all dataset resources.
// Owns Metadata and the full CRUD flow (Schema/Configure/Create/Read/Update/Delete
// are promoted from resourceBase).
type BaseDatasetResource struct {
	resourceBase[DatasetResourceBackend]
}

func (r *BaseDatasetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dataset"
}

// BaseSchema implements ResourceBackend.baseSchema for dataset resources.
// Consumers who embed BaseDatasetResource inherit this automatically.
func (r *BaseDatasetResource) BaseSchema() schema.Schema {
	return GenerateDatasetSchema(r.Backend.Capability())
}
