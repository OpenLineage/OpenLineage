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

// JobResourceBackend defines the consumer-specific operations for job resources.
// The 7 shared methods are inherited from ResourceBackend; only Capability() is added here.
type JobResourceBackend interface {
	ResourceBackend
	// Capability declares which OL facets this consumer supports.
	Capability() JobCapability
}

// BaseJobResource is the generic base for all job resources.
// Directly owns Metadata and BaseSchema; Configure/Create/Read/Update/Delete/Schema
// are promoted from resourceBase.
type BaseJobResource struct {
	resourceBase[JobResourceBackend]
}

func (r *BaseJobResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_job"
}

// BaseSchema implements ResourceBackend.BaseSchema for job resources.
// Consumers who embed BaseJobResource inherit this automatically.
func (r *BaseJobResource) BaseSchema() schema.Schema {
	return GenerateJobSchema(r.Backend.Capability())
}
