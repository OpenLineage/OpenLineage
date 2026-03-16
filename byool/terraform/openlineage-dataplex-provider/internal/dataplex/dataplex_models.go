/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package dataplex

import (
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/tomasznazarewicz/openlineage-terraform-resources/ol"
)

// DataplexJobModel is the top-level state struct for the openlineage_job resource.
// ol.JobResourceModel and DataplexState are embedded (no tfsdk tag) so the
// framework promotes all their fields into this struct's attribute namespace.
type DataplexJobModel struct {
	ol.JobResourceModel // embedded — promotes id, run_id, namespace, name, inputs, outputs, …
	DataplexState       // embedded — promotes process_name, run_name, lineage_event_name, update_time
}

type DataplexState struct {
	// ProcessName is the full GCP resource name of the Dataplex Process entity.
	// e.g. "projects/my-project/locations/us-central1/processes/abc123"
	// Stable across multiple applies — the same process is reused for all runs
	// of the same job (same namespace+name).
	ProcessName types.String `tfsdk:"process_name"`

	// RunName is the full GCP resource name of the Dataplex Run entity
	// created for the most recent emission.
	// e.g. "projects/.../processes/abc123/runs/xyz"
	// Changes on every apply — each emit creates a new Run.
	RunName types.String `tfsdk:"run_name"`

	// LineageEventName is the full GCP resource name of the Dataplex LineageEvent
	// created for the most recent emission.
	// e.g. "projects/.../lineageEvents/def456"
	// Changes on every apply.
	LineageEventName types.String `tfsdk:"lineage_event_name"`

	// UpdateTime is the end time (or start time if not yet complete) of the
	// most recent Run, in ISO 8601 format.
	UpdateTime types.String `tfsdk:"update_time"`
}
