/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package dataplex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	// lineage is the GCP Data Catalog Lineage API client library.
	// It speaks gRPC under the hood and handles auth, retries, etc.
	lineage "cloud.google.com/go/datacatalog/lineage/apiv1"

	// lineagepb contains the protobuf-generated request/response types
	// for the Lineage API (e.g. GetProcessRequest, ListRunsRequest).
	lineagepb "cloud.google.com/go/datacatalog/lineage/apiv1/lineagepb"

	// iterator is a Google API helper — it lets us range over paginated
	// list results (ListProcesses, ListRuns) without manually handling
	// page tokens. iterator.Done signals the end of results.
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	// codes and status let us inspect gRPC error types.
	// We use them to distinguish "not found" (404) from real errors.
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// structpb lets us convert a Go map[string]any into a protobuf Struct,
	// which is what ProcessOpenLineageRunEvent expects as its payload.
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tomasznazarewicz/openlineage-terraform-resources/ol"
)

// dataplexClient wraps the GCP Lineage API client.
// It holds the underlying gRPC client and the parent resource path
// (projects/{project}/locations/{region}) that prefixes every API call.
type dataplexClient struct {
	client *lineage.Client
	parent string // e.g. "projects/my-project/locations/us-central1"
}

// newDataplexClient creates and connects a dataplexClient.
// It uses Application Default Credentials by default; if credentialsFile
// is provided it uses that service account key file instead.
func newDataplexClient(ctx context.Context, projectID, location, credentialsFile string) (*dataplexClient, error) {
	var opts []option.ClientOption
	if credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}

	// lineage.NewClient opens the gRPC connection to the Dataplex Lineage API.
	client, err := lineage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create GCP lineage client: %w", err)
	}

	// parent is the resource path all sub-resources (processes, runs, events) live under.
	parent := fmt.Sprintf("projects/%s/locations/%s", projectID, location)

	return &dataplexClient{
		client: client,
		parent: parent,
	}, nil
}

// ── Result types ─────────────────────────────────────────────────────────────

// processInfo is a small struct we use internally to carry just the fields
// we care about from a Dataplex Process resource.
type processInfo struct {
	ProcessName    string // full GCP resource name, e.g. "projects/.../processes/abc"
	DisplayName    string // human-readable name Dataplex assigns, e.g. "namespace:jobName"
	OriginName     string // the origin name set when we emitted the OL event, e.g. "openlineage-byol-dataplex-0.1.0"
	OriginVerified bool   // true if origin matches what we expect from this dataplex
}

// runInfo carries the fields we need from a Dataplex Run resource.
type runInfo struct {
	RunName   string    // full GCP resource name of the Run
	State     string    // e.g. "COMPLETED", "FAILED", "RUNNING"
	StartTime time.Time // when this Run started
	EndTime   time.Time // when this Run finished (zero if still running)
}

// emitResult holds the three resource names that Dataplex returns when we
// send an OpenLineage event via ProcessOpenLineageRunEvent.
// These are the IDs we store in Terraform state so we can reference/delete them later.
type emitResult struct {
	ProcessName       string   // the Dataplex Process this event was attributed to
	RunName           string   // the Dataplex Run that was created/updated
	LineageEventNames []string // one or more LineageEvent resources created
}

// ── Core operations ───────────────────────────────────────────────────────────

// emitAndCapture is the main "write" operation.
//
// Flow:
//  1. Marshal the OpenLineage RunEvent to JSON (the OL client gives us a Go struct)
//  2. Convert that JSON into a protobuf Struct (what the Dataplex API accepts)
//  3. Call ProcessOpenLineageRunEvent — Dataplex parses the OL event, creates/updates
//     a Process, a Run, and one or more LineageEvent resources
//  4. Return the resource names from the response so we can store them in state
//
// This replaces the old olClient.Emit() approach which discarded the response.
// By calling the Dataplex gRPC API directly we get back the exact resource names.
func (d *dataplexClient) emitAndCapture(ctx context.Context, event any) (*emitResult, error) {
	// Step 1: Go struct → JSON bytes
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// Step 2: JSON bytes → map[string]any → protobuf Struct
	// The protobuf Struct type is a generic key-value container that can hold
	// any JSON-compatible data. The Dataplex API uses it to accept OL events
	// without knowing the OL schema at compile time.
	var eventMap map[string]any
	if err := json.Unmarshal(eventJSON, &eventMap); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}

	olStruct, err := structpb.NewStruct(eventMap)
	if err != nil {
		return nil, fmt.Errorf("convert event to protobuf struct: %w", err)
	}

	// Step 3: send to Dataplex
	resp, err := d.client.ProcessOpenLineageRunEvent(ctx, &lineagepb.ProcessOpenLineageRunEventRequest{
		Parent:      d.parent, // which project/location to create the entities in
		OpenLineage: olStruct, // the OL event payload
	})
	if err != nil {
		return nil, fmt.Errorf("emit event: %w", err)
	}

	// Step 4: extract and return the resource names from the response
	return &emitResult{
		ProcessName:       resp.GetProcess(),       // e.g. "projects/.../processes/abc"
		RunName:           resp.GetRun(),           // e.g. "projects/.../processes/abc/runs/xyz"
		LineageEventNames: resp.GetLineageEvents(), // e.g. ["projects/.../lineageEvents/123"]
	}, nil
}

// getProcess fetches a single Dataplex Process by its exact GCP resource name.
//
// This is used in Read to verify the process still exists (drift detection).
// If Dataplex returns a 404 (the process was deleted outside of Terraform),
// we return nil with no error — the caller interprets nil as "not found"
// and calls resp.State.RemoveResource() to tell Terraform to re-create it.
//
// We also verify the process origin — Dataplex sets Origin.SourceType = CUSTOM
// and Origin.Name = the value from the GcpLineage facet we sent. If that doesn't
// match, the process exists but wasn't created by this dataplex (e.g. a collision
// with a manually created process), which we surface via OriginVerified = false.
func (d *dataplexClient) getProcess(ctx context.Context, processName string) (*processInfo, error) {
	process, err := d.client.GetProcess(ctx, &lineagepb.GetProcessRequest{Name: processName})
	if err != nil {
		// codes.NotFound is the gRPC equivalent of HTTP 404.
		// We treat "not found" as a normal situation (drift), not an error.
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get process: %w", err)
	}

	info := &processInfo{
		ProcessName: process.GetName(),
		DisplayName: process.GetDisplayName(),
	}

	// Check the origin set by Dataplex when it processed our OL event.
	// Origin.SourceType == CUSTOM (enum value 1) means it came from a custom integration.
	// Origin.Name should match the value we put in the GcpLineage facet.
	if origin := process.GetOrigin(); origin != nil {
		info.OriginName = origin.GetName()
		info.OriginVerified = origin.GetSourceType() == lineagepb.Origin_CUSTOM &&
			origin.GetName() == ol.ProviderOriginName
	}

	return info, nil
}

// searchProcess scans all Processes in the parent location and returns the one
// that matches the given OpenLineage namespace and job name.
//
// This is an expensive O(n) scan and is only used during `terraform import`,
// where we don't yet have a stored process_name to look up directly.
// For all normal Read operations we use getProcess() with the stored name.
func (d *dataplexClient) searchProcess(ctx context.Context, namespace, jobName string) (*processInfo, error) {
	// ListProcesses returns a paginated iterator — the iterator library
	// fetches pages automatically as we call it.Next().
	it := d.client.ListProcesses(ctx, &lineagepb.ListProcessesRequest{Parent: d.parent})

	for {
		process, err := it.Next()
		if err == iterator.Done {
			// iterator.Done is not an error — it just means we've seen all pages.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list processes: %w", err)
		}

		if matchesOrigin(process, namespace, jobName) {
			info := &processInfo{
				ProcessName: process.GetName(),
				DisplayName: process.GetDisplayName(),
			}
			if origin := process.GetOrigin(); origin != nil {
				info.OriginName = origin.GetName()
				info.OriginVerified = origin.GetSourceType() == lineagepb.Origin_CUSTOM &&
					origin.GetName() == ol.ProviderOriginName
			}
			return info, nil
		}
	}

	return nil, nil // no matching process found
}

// matchesOrigin checks whether a given Dataplex Process was created from an
// OpenLineage event with the specified namespace and job name.
//
// Matching priority:
//  1. Origin check — if the process has SourceType=CUSTOM and our ol.ProviderOriginName,
//     it was definitely created by this dataplex; then match display_name against
//     the expected "namespace:jobName" format that Dataplex sets from the OL event.
//  2. Attribute check — explicit OL origin attributes (may not always be present).
//  3. display_name fallback — Dataplex sometimes sets it to just the job name.
func matchesOrigin(process *lineagepb.Process, namespace, jobName string) bool {
	displayName := process.GetDisplayName()
	expectedDisplayName := fmt.Sprintf("%s:%s", namespace, jobName)

	// Primary: origin-based match — most reliable since it's set by Dataplex
	// from the GcpLineage facet we send in the OL event.
	if origin := process.GetOrigin(); origin != nil {
		if origin.GetSourceType() == lineagepb.Origin_CUSTOM &&
			origin.GetName() == ol.ProviderOriginName {
			// Provider origin confirmed — now match by display_name which Dataplex
			// sets to "namespace:jobName" from the OL event job identity.
			return displayName == expectedDisplayName || displayName == jobName
		}
	}

	// Secondary: explicit OL attributes stored by some API versions
	if attrs := process.GetAttributes(); attrs != nil {
		if getStringAttribute(attrs, "openlineage_namespace") == namespace &&
			getStringAttribute(attrs, "openlineage_job") == jobName {
			return true
		}
	}

	// Fallback: display_name only — least specific, may match unrelated processes
	return displayName == expectedDisplayName || displayName == jobName
}

// getStringAttribute safely extracts a string from a map[string]*structpb.Value.
// The structpb.Value is a tagged union — we must type-assert the Kind field
// to get the actual string out. Returns "" if the key is missing or not a string.
func getStringAttribute(attrs map[string]*structpb.Value, key string) string {
	if v, ok := attrs[key]; ok && v != nil {
		// GetKind() returns the underlying type as an interface.
		// *structpb.Value_StringValue is the concrete type for string values.
		if sv, ok := v.GetKind().(*structpb.Value_StringValue); ok {
			return sv.StringValue
		}
	}
	return ""
}

// getLatestRun lists all Runs under a Process and returns the most recently started one.
//
// We use this after emitting an event (Create/Update) to read back the run_state,
// and also during Read to refresh the state in case it has changed.
// Runs are append-only in Dataplex — each emit creates a new Run, so "latest by
// start_time" gives us the one corresponding to our most recent emission.
func (d *dataplexClient) getLatestRun(ctx context.Context, processName string) (*runInfo, error) {
	// processName becomes the parent path for ListRuns
	it := d.client.ListRuns(ctx, &lineagepb.ListRunsRequest{Parent: processName})

	var latest *lineagepb.Run
	var latestTime time.Time

	for {
		run, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list runs: %w", err)
		}

		// Keep track of whichever run started most recently
		if t := run.GetStartTime().AsTime(); latest == nil || t.After(latestTime) {
			latest = run
			latestTime = t
		}
	}

	if latest == nil {
		return nil, nil // process exists but has no runs yet
	}

	info := &runInfo{
		RunName: latest.GetName(),
		State:   latest.GetState().String(), // converts the proto enum to e.g. "COMPLETED"
	}
	// GetStartTime() / GetEndTime() return *timestamppb.Timestamp.
	// AsTime() converts to a standard time.Time. The nil check guards against
	// runs that haven't started or finished yet.
	if st := latest.GetStartTime(); st != nil {
		info.StartTime = st.AsTime()
	}
	if et := latest.GetEndTime(); et != nil {
		info.EndTime = et.AsTime()
	}

	return info, nil
}

// deleteProcess deletes a Dataplex Process and all its child Runs and LineageEvents.
//
// DeleteProcess is a long-running operation (LRO) — the API returns an Operation
// object immediately, and the actual deletion happens asynchronously.
// op.Wait() blocks until the deletion completes or fails.
//
// AllowMissing: true means Dataplex won't return an error if the process is
// already gone — safe to call even if it was deleted outside of Terraform.
func (d *dataplexClient) deleteProcess(ctx context.Context, processName string) error {
	op, err := d.client.DeleteProcess(ctx, &lineagepb.DeleteProcessRequest{
		Name:         processName,
		AllowMissing: true, // treat "already deleted" as success
	})
	if err != nil {
		return fmt.Errorf("delete process: %w", err)
	}

	// Block until the LRO finishes. This can take a few seconds.
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for process deletion: %w", err)
	}

	return nil
}

// close cleanly shuts down the underlying gRPC connection.
// Should be called when the dataplex is done with this client.
func (d *dataplexClient) close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
