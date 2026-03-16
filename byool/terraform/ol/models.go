/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import "github.com/hashicorp/terraform-plugin-framework/types"

// JobResourceModel is the top-level state struct for the openlineage_job resource.
// OLJobConfig is embedded (no tfsdk tag) so the framework promotes its fields
// directly into this struct's attribute/block namespace.
type JobResourceModel struct {
	ID    types.String `tfsdk:"id"`
	RunID types.String `tfsdk:"run_id"`

	OLJobConfig                 // embedded — fields promoted: namespace, name, description, job_type, …
	Inputs      []OLInputModel  `tfsdk:"inputs"`
	Outputs     []OLOutputModel `tfsdk:"outputs"`
}

// OLInputModel represents a single input dataset.
// DatasetModel is embedded so its fields (namespace, name, symlinks, catalog, …)
// are promoted directly into the inputs block schema.
type OLInputModel struct {
	DatasetModel // embedded
}

// OLOutputModel represents a single output dataset.
// DatasetModel is embedded; column_lineage is output-only.
type OLOutputModel struct {
	DatasetModel                             // embedded
	ColumnLineage *ColumnLineageDatasetModel `tfsdk:"column_lineage"`
}

// DatasetResourceModel is the top-level state struct for a standalone dataset resource.
type DatasetResourceModel struct {
	ID           types.String `tfsdk:"id"`
	DatasetModel              // embedded
}
