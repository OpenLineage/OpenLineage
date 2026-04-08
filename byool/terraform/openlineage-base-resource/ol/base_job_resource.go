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

// JobResourceBackend defines the consumer-specific operations that BaseJobResource
// delegates to. Each consumer (Dataplex, Marquez, …) implements this interface.
// The 7 shared methods are inherited from ResourceBackend; only Capability() is added here.
//
// Java analogy:
//
//	interface JobResourceBackend extends ResourceBackend {
//	    JobCapability capability();
//	}
type JobResourceBackend interface {
	ResourceBackend
	// Capability declares which OL facets this consumer supports.
	Capability() JobCapability
}

// BaseJobResource is the generic base for all job resources.
// Owns Metadata, Schema, and the full CRUD flow (Configure/Create/Read/Update/Delete
// are promoted from resourceBase).
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
	resourceBase[JobResourceBackend]
}

func (r *BaseJobResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_job"
}

// baseSchema implements ResourceBackend.baseSchema for job resources.
// Consumers who embed BaseJobResource inherit this automatically.
func (r *BaseJobResource) baseSchema() schema.Schema {
	return GenerateJobSchema(r.Backend.Capability())
}
