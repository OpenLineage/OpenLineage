/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package file

import (
	"github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// FileState holds the consumer-specific computed fields written back to
// Terraform state after each emit. Shared by all three resource types.
type FileState struct {
	// FilePath is the full path to the JSON file on disk.
	// Derived deterministically: {output_dir}/{prefix}__{namespace}__{name}.json
	FilePath types.String `tfsdk:"file_path"`

	// LastEmitted is the RFC3339 timestamp of the most recent write to disk.
	LastEmitted types.String `tfsdk:"last_emitted"`

	// EmitCount is the number of times this resource's lineage has been emitted.
	EmitCount types.Int64 `tfsdk:"emit_count"`
}

// FileRunModel is the Terraform state struct for the openlineage_run resource.
// Emits an OpenLineage RunEvent — a job execution with a unique run ID,
// inputs, and outputs.
type FileRunModel struct {
	ol.JobResourceModel // promoted: namespace, name, description, job_type, ownership, inputs, outputs, …
	FileState           // promoted: file_path, last_emitted, emit_count
}

// FileJobModel is the Terraform state struct for the openlineage_job resource.
// Emits an OpenLineage JobEvent — static job metadata without a run ID.
type FileJobModel struct {
	ol.JobResourceModel // promoted: namespace, name, description, job_type, ownership, inputs, outputs, …
	FileState           // promoted: file_path, last_emitted, emit_count
}

// FileDatasetModel is the Terraform state struct for the openlineage_dataset resource.
// Emits an OpenLineage DatasetEvent — standalone dataset metadata.
type FileDatasetModel struct {
	ol.DatasetResourceModel // promoted: namespace, name, symlinks, schema, catalog, …
	FileState               // promoted: file_path, last_emitted, emit_count
}
