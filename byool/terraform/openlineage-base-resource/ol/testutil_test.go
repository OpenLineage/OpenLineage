/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package ol

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

const testProducer = "https://github.com/OpenLineage/OpenLineage/tree/main/byool/terraform/openlineage-base-resource/test"

// recorder captures what the test provider's backends do. It is shared by all provider
// instances of one test, because terraform-plugin-testing starts a new provider per command.
type recorder struct {
	mu      sync.Mutex
	events  map[string][]map[string]any // "job/<ns>/<name>" or "dataset/<ns>/<name>" → emitted events, oldest first
	deletes []string
}

func newRecorder() *recorder {
	return &recorder{events: map[string][]map[string]any{}}
}

func (r *recorder) emit(key string, event any) diag.Diagnostics {
	var diags diag.Diagnostics
	raw, err := json.Marshal(event)
	if err != nil {
		diags.AddError("marshal event", err.Error())
		return diags
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		diags.AddError("unmarshal event", err.Error())
		return diags
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events[key] = append(r.events[key], decoded)
	return diags
}

func (r *recorder) deleted(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletes = append(r.deletes, key)
}

// last returns the latest event emitted for key, or nil.
func (r *recorder) last(key string) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := r.events[key]
	if len(evs) == 0 {
		return nil
	}
	return evs[len(evs)-1]
}

func (r *recorder) deletedKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.deletes...)
}

// backendCommon implements the parts of ResourceBackend that don't depend on the resource type.
type backendCommon struct{ rec *recorder }

func (backendCommon) ConsumerConfigure(context.Context, resource.ConfigureRequest, *resource.ConfigureResponse) {
}
func (backendCommon) ConsumerAttributes() map[string]schema.Attribute { return nil }
func (backendCommon) ConsumerBlocks() map[string]schema.Block         { return nil }
func (backendCommon) ConsumerRead(context.Context, any) (bool, diag.Diagnostics) {
	return true, nil
}

type recordingJobBackend struct {
	backendCommon
	c JobCapability
}

func (b *recordingJobBackend) Capability() JobCapability { return b.c }
func (b *recordingJobBackend) NewModel() any             { return &JobResourceModel{} }

func (b *recordingJobBackend) ConsumerEmit(_ context.Context, model any) diag.Diagnostics {
	m := model.(*JobResourceModel)
	var diags diag.Diagnostics
	event := NewJobEventBuilder(&diags, b.c, testProducer).BuildJobEvent(m)
	if diags.HasError() {
		return diags
	}
	return b.rec.emit(jobKey(m.Namespace.ValueString(), m.Name.ValueString()), event)
}

func (b *recordingJobBackend) ConsumerDelete(_ context.Context, model any) diag.Diagnostics {
	m := model.(*JobResourceModel)
	b.rec.deleted(jobKey(m.Namespace.ValueString(), m.Name.ValueString()))
	return nil
}

type recordingDatasetBackend struct {
	backendCommon
	c DatasetCapability
}

func (b *recordingDatasetBackend) Capability() DatasetCapability { return b.c }
func (b *recordingDatasetBackend) NewModel() any                 { return &DatasetResourceModel{} }

func (b *recordingDatasetBackend) ConsumerEmit(_ context.Context, model any) diag.Diagnostics {
	m := model.(*DatasetResourceModel)
	var diags diag.Diagnostics
	event := NewDatasetEventBuilder(&diags, b.c, testProducer).BuildDatasetEvent(m)
	if diags.HasError() {
		return diags
	}
	return b.rec.emit(datasetKey(m.Namespace.ValueString(), m.Name.ValueString()), event)
}

func (b *recordingDatasetBackend) ConsumerDelete(_ context.Context, model any) diag.Diagnostics {
	m := model.(*DatasetResourceModel)
	b.rec.deleted(datasetKey(m.Namespace.ValueString(), m.Name.ValueString()))
	return nil
}

func jobKey(ns, name string) string     { return "job/" + ns + "/" + name }
func datasetKey(ns, name string) string { return "dataset/" + ns + "/" + name }

// testProvider is a minimal provider ("openlineage") exposing openlineage_job and
// openlineage_dataset over recording backends.
type testProvider struct {
	rec   *recorder
	jobC  JobCapability
	dataC DatasetCapability
}

var _ provider.Provider = (*testProvider)(nil)

func (p *testProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "openlineage"
}

func (p *testProvider) Schema(context.Context, provider.SchemaRequest, *provider.SchemaResponse) {}

func (p *testProvider) Configure(context.Context, provider.ConfigureRequest, *provider.ConfigureResponse) {
}

func (p *testProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		func() resource.Resource {
			return NewJobResource(&recordingJobBackend{backendCommon{p.rec}, p.jobC})
		},
		func() resource.Resource {
			return NewDatasetResource(&recordingDatasetBackend{backendCommon{p.rec}, p.dataC})
		},
	}
}

func (p *testProvider) DataSources(context.Context) []func() datasource.DataSource { return nil }

// providerFactories returns the factories for resource.TestCase.ProtoV6ProviderFactories.
func providerFactories(rec *recorder, jobC JobCapability, dataC DatasetCapability) map[string]func() (tfprotov6.ProviderServer, error) {
	return map[string]func() (tfprotov6.ProviderServer, error){
		"openlineage": providerserver.NewProtocol6WithError(&testProvider{rec: rec, jobC: jobC, dataC: dataC}),
	}
}

// allJobFacets / allDatasetFacets list every generated facet, so capabilities built from them
// cover new facets automatically.
func allJobFacets() []JobFacet {
	out := make([]JobFacet, 0, len(JobFacetSchemas))
	for _, s := range JobFacetSchemas {
		out = append(out, s.JobFacet)
	}
	return out
}

func allDatasetFacets() []DatasetFacet {
	out := make([]DatasetFacet, 0, len(DatasetFacetSchemas)+1)
	for _, s := range DatasetFacetSchemas {
		out = append(out, s.DatasetFacet)
	}
	return append(out, ColumnLineageDatasetFacet)
}

func allOnJobCapability() JobCapability {
	return EmptyJobCapability().WithFacetEnabled(allJobFacets()...).WithDatasetFacetEnabled(allDatasetFacets()...)
}

func allOnDatasetCapability() DatasetCapability {
	return EmptyDatasetCapability().WithFacetEnabled(allDatasetFacets()...)
}

// testConfig reads testdata/<name>.
func testConfig(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	return string(b)
}
