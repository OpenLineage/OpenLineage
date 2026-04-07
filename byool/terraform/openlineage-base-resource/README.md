# `ol` package

Generic building blocks for Terraform resources that use OpenLineage-shaped configuration.

This package centralizes the largest possible generic part of OL Terraform resources: shared models, schema generation, capability-driven facet handling, and base CRUD orchestration.

## Goal

Provide a reusable base where OpenLineage config is as interchangeable as possible across resources and consumers.

## Architecture (Strategy Pattern)

`BaseJobResource` and `BaseDatasetResource` own Terraform lifecycle flow, while consumer-specific behavior is delegated to backend interfaces.

For job resources, `BaseJobResource` delegates to `JobResourceBackend`:

- `Capability()`
- `ConsumerConfigure(...)`
- `ConsumerAttributes()`
- `ConsumerBlocks()`
- `NewModel()`
- `ConsumerEmit(...)`
- `ConsumerRead(...)`
- `ConsumerDelete(...)`

Typical constructor pattern:

```go
func NewMyJobResource() resource.Resource {
    r := &MyJobResource{}
    r.Backend = r // self-reference: concrete resource implements backend
    return r
}
```

`BaseJobResource.Schema()` generates the generic schema from capability and then merges consumer-specific attributes/blocks.

## Data Models

Top-level Terraform state models:

- `JobResourceModel` in `models.go`
- `DatasetResourceModel` in `models.go`

OpenLineage-shaped nested models:

- `OLJobConfig` in `ol_models.go`
- `DatasetModel` in `ol_models.go`
- facet models in `ol_models.go` (job facets, dataset facets, column lineage)

Important embedding rule:

- embedded structs like `OLJobConfig` and `DatasetModel` must stay embedded without a `tfsdk` tag,
- Terraform framework promotes embedded fields into the parent namespace,
- adding `tfsdk` tags on embedded fields can produce `invalid tfsdk tag` errors.

## Capability Model

Capabilities declare supported facets for a given consumer/resource:

- `EmptyJobCapability()`
- `EmptyDatasetCapability()`
- `WithFacetEnabled(...)`

Facet constants are defined in `capability.go` and cover both job and dataset facets.

Facets that are not explicitly enabled are represented as compatibility stubs in generated schema:

- block remains present,
- leaf attributes become `Optional + Computed`,
- copied config is accepted and safely ignored by unsupported consumers.

This behavior is what keeps config portable across consumers with different facet support.

## Schema Generation

- `GenerateJobSchema(cap JobCapability)` builds `openlineage_job` schema.
- `GenerateDatasetSchema(cap DatasetCapability)` builds `openlineage_dataset` schema.

Current behavior:

- identity attributes are always present (`namespace`, `name`, plus `description` for job),
- job facet blocks are included as active blocks or stubs depending on capability,
- `inputs` and `outputs` are always present and use capability-aware dataset facet blocks,
- standalone dataset schema is also capability-driven.

Consumer-specific schema parts are merged after generation by base resources.

## Event Builder

`BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent`,
`BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent`, and
`BuildDatasetEvent(data *DatasetResourceModel) *openlineage.DatasetEvent`
map Terraform models to OpenLineage events.

Current behavior:

- `BuildJobEvent` builds the shared job payload,
- `BuildRunEvent` is a compatibility wrapper for consumers that do not support static events yet,
- `BuildRunEvent` wraps `BuildJobEvent`, sets `eventType = COMPLETE`, and generates `run.runId` internally,
- `BuildRunEvent` does **not** populate run facets,
- `BuildDatasetEvent` builds a standalone dataset event from `DatasetResourceModel`,
- event type is `COMPLETE`,
- job identity is built from `namespace` and `name`,
- `inputs` and `outputs` are mapped to OL dataset elements with supported facets,
- dataset events and dataset elements reuse the same dataset-facet mapping helpers.

## Package Files

- `base_job_resource.go` - generic CRUD/resource flow for job resources
- `base_dataset_resource.go` - generic CRUD/resource flow for dataset resources
- `models.go` - top-level Terraform state models
- `ol_models.go` - OpenLineage-shaped models and facet structures
- `capability.go` - facet enum and capability types
- `schema_generator.go` - capability-driven Terraform schema builders
- `event_builder.go` - model to OpenLineage event mapping

## For Provider Authors

This section is a step-by-step guide to building a Terraform provider that targets a new
OpenLineage consumer (Marquez, Atlas, etc.) using this package as a base.

The full provider-author guide and code templates are included in the steps below.

### Step 1 — Define a consumer model

Create a model that embeds `ol.JobResourceModel` (or `ol.DatasetResourceModel`) and adds
consumer-specific computed fields.

**Critical:** embedded structs must have no `tfsdk` tag. The framework promotes embedded
fields into the parent namespace automatically. A tag causes `invalid tfsdk tag` errors.

```go
// ✓ correct
type MyJobModel struct {
    ol.JobResourceModel  // no tag — fields promoted: namespace, name, description, inputs, outputs, …
    MyState              // no tag — consumer-specific computed fields
}

type MyState struct {
    ResourceID types.String `tfsdk:"my_resource_id"`
}
```

### Step 2 — Implement `JobResourceBackend`

Embed `ol.BaseJobResource`, point `Backend` at self, and implement the interface.
A compile-time check catches missing methods early.

```go
var _ ol.JobResourceBackend = &MyJobResource{}

type MyJobResource struct {
    ol.BaseJobResource
    client *MyClient
}

func NewMyJobResource() resource.Resource {
    r := &MyJobResource{}
    r.Backend = r  // required: self-reference
    return r
}
```

### Step 3 — Declare `Capability`

Start from `EmptyJobCapability()` and enable only what the target system supports.
Facets that are not enabled remain in the schema as `Optional + Computed` stubs —
config copied from another consumer is accepted without error, values are ignored.

```go
func (r *MyJobResource) Capability() ol.JobCapability {
    return ol.EmptyJobCapability().WithFacetEnabled(
        ol.FacetJobType,
        ol.FacetJobOwnership,
        ol.FacetDatasetSymlinks,
        ol.FacetDatasetCatalog,
    )
}
```

### Step 4 — Add consumer schema attributes

Return only attributes that are unique to the consumer (e.g. system-assigned IDs).
Do **not** redeclare `namespace`, `name`, `description`, `inputs`, `outputs` — those
are already present in the generated schema.

```go
func (r *MyJobResource) ConsumerAttributes() map[string]schema.Attribute {
    return map[string]schema.Attribute{
        "my_resource_id": schema.StringAttribute{
            Computed:    true,
            Description: "ID assigned by the target system after emission.",
        },
    }
}

func (r *MyJobResource) ConsumerBlocks() map[string]schema.Block {
    return map[string]schema.Block{}
}
```

### Step 5 — Implement `ConsumerEmit`

Build the OL event from the generic model, call the consumer API, and write
consumer-specific IDs back into the model state.

```go
func (r *MyJobResource) ConsumerEmit(ctx context.Context, modelAny any, runID uuid.UUID) diag.Diagnostics {
    var diags diag.Diagnostics
    model := modelAny.(*MyJobModel)

    event := ol.BuildRunEvent(&model.JobResourceModel)
    emittedRunID := event.Run.RunID

    result, err := r.client.Emit(ctx, event)
    if err != nil {
        diags.AddError("Emission failed", err.Error())
        return diags
    }

    model.ResourceID = types.StringValue(result.ID)
    model.EmittedRunID = types.StringValue(emittedRunID)
    return diags
}
```

### Step 6 — Implement `ConsumerRead`

Return `false` (not an error) when the entity no longer exists — this signals drift
to `BaseJobResource`, which calls `RemoveResource()` to trigger re-create on next plan.

```go
func (r *MyJobResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
    model := modelAny.(*MyJobModel)
    exists, err := r.client.Exists(ctx, model.Namespace.ValueString(), model.Name.ValueString())
    if err != nil {
        return false, diag.Diagnostics{diag.NewErrorDiagnostic("Read failed", err.Error())}
    }
    return exists, nil
}
```

### Step 7 — Implement `ConsumerDelete`

Treat "not found" as success — idempotent deletes prevent errors on re-runs.

```go
func (r *MyJobResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
    model := modelAny.(*MyJobModel)
    err := r.client.Delete(ctx, model.Namespace.ValueString(), model.Name.ValueString())
    if err != nil && !isNotFound(err) {
        return diag.Diagnostics{diag.NewErrorDiagnostic("Delete failed", err.Error())}
    }
    return nil
}
```

### `NewModel` factory

Must return a pointer to the concrete consumer model, not `ol.JobResourceModel`:

```go
func (r *MyJobResource) NewModel() any {
    return &MyJobModel{}
}
```

### Common mistakes

| Symptom | Cause | Fix |
|---|---|---|
| `invalid tfsdk tag` | embedded struct has a `tfsdk` tag | remove the tag |
| `nil pointer dereference` in emit | `r.Backend = r` missing | add self-reference in constructor |
| Schema missing fields from embedded model | consumer redeclares generic keys | remove `namespace`, `name`, `description`, `inputs`, `outputs` from `ConsumerAttributes()` |
| Drift not detected | `ConsumerRead` returns `error` instead of `false` for missing resource | return `false, nil` when not found |
| Build fails after package update | consumer still calls `BuildRunEvent(..., runID)` | call `BuildRunEvent(&model.JobResourceModel)` and read `event.Run.RunID` from the returned event if needed |
