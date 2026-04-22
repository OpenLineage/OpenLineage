# `ol` package

Generic building blocks for Terraform resources that use OpenLineage-shaped configuration.

This package centralizes the largest possible generic part of OL Terraform resources: shared models, schema generation, capability-driven facet handling, and base CRUD orchestration.

## Goal

Provide a reusable base where OpenLineage config is as interchangeable as possible across resources and consumers.

## Architecture (Strategy Pattern)

The package uses a three-layer design:

```
resourceBase[B]          — generic; holds Backend, CRUD lifecycle, nil-check
  └─ BaseJobResource     — job-specific: Metadata ("_job"), BaseSchema (GenerateJobSchema)
  └─ BaseDatasetResource — dataset-specific: Metadata ("_dataset"), BaseSchema (GenerateDatasetSchema)

ResourceBackend          — shared interface (8 methods: 7 lifecycle + BaseSchema)
  └─ JobResourceBackend  — adds Capability() JobCapability
  └─ DatasetResourceBackend — adds Capability() DatasetCapability
```

`resourceBase[B]` owns all five CRUD methods (`Configure`, `Create`, `Read`, `Update`, `Delete`) and promotes them to both base types via embedding. `BaseJobResource` and `BaseDatasetResource` only add `Metadata` and `BaseSchema`.

Consumer-specific behaviour is entirely delegated through the `Backend` field:

| Method | Defined on | Purpose |
|---|---|---|
| `Capability()` | `JobResourceBackend` / `DatasetResourceBackend` | declares supported facets |
| `ConsumerConfigure(...)` | `ResourceBackend` | initialises client from provider config |
| `ConsumerAttributes()` | `ResourceBackend` | extra schema attributes (e.g. system IDs) |
| `ConsumerBlocks()` | `ResourceBackend` | extra schema blocks |
| `NewModel()` | `ResourceBackend` | allocates a fresh state model |
| `ConsumerEmit(...)` | `ResourceBackend` | builds and sends the OL event |
| `ConsumerRead(...)` | `ResourceBackend` | checks existence, refreshes computed fields |
| `ConsumerDelete(...)` | `ResourceBackend` | removes the entity from the consumer |
| `BaseSchema()` | `BaseJobResource` / `BaseDatasetResource` | returns capability-driven schema; inherited via embedding, **not** implemented by consumers |

Typical constructor pattern:

```go
func NewMyJobResource() resource.Resource {
    r := &MyJobResource{}
    r.Backend = r // self-reference: concrete resource implements backend
    return r
}
```

## Data Models

Top-level Terraform state models:

- `JobResourceModel` in `models.go`
- `DatasetResourceModel` in `models.go`

OpenLineage-shaped nested models in `ol_models.go`:

- `OLJobConfig` — job identity + all job facet blocks
- `DatasetModel` — dataset identity + all dataset facet blocks (shared by inputs, outputs, and standalone dataset resources)
- individual facet structs (job facets, dataset facets, column lineage)

**Facet wrapper models** — three facets use an outer `SingleNestedBlock` wrapping an inner list, consistent with the `OwnershipJobModel / owners` pattern:

| Model | Outer block | Inner list key | Element model |
|---|---|---|---|
| `TagsJobFacetModel` | `tags` | `tag` | `TagsJobModel` |
| `TagsDatasetFacetModel` | `tags` | `tag` | `TagsDatasetModel` |
| `SymlinksDatasetFacetModel` | `symlinks` | `symlink` | `IdentifierModel` |

Important embedding rule:

- embedded structs like `OLJobConfig` and `DatasetModel` must stay embedded without a `tfsdk` tag,
- Terraform framework promotes embedded fields into the parent namespace,
- adding `tfsdk` tags on embedded fields can produce `invalid tfsdk tag` errors.

## Capability Model

Capabilities declare supported facets for a given consumer/resource:

- `EmptyJobCapability()`
- `EmptyDatasetCapability()`
- `JobCapability.WithFacetEnabled(...JobFacet)` — enables job-level facets
- `JobCapability.WithDatasetFacetEnabled(...DatasetFacet)` — enables dataset facets for the job's inputs/outputs
- `DatasetCapability.WithFacetEnabled(...DatasetFacet)` — enables dataset facets; job facets are not accepted (compile-time error)

Facet constants are defined in `capability.go` and split by type:
`FacetJob*` constants are of type `JobFacet`; `FacetDataset*` constants are of type `DatasetFacet`.
Passing a `JobFacet` to `DatasetCapability.WithFacetEnabled` is caught at compile time.

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

Consumer-specific schema parts are merged after generation by `resourceBase.mergeConsumerSchema`.

## Event Builder

The `OpenLineageEventBuilder` struct assembles OpenLineage events from Terraform models.
It is created via constructor functions that bind a `Diagnostics` sink and a capability:

```go
builder := ol.NewJobEventBuilder(&diags, cap)     // for job resources
builder := ol.NewDatasetEventBuilder(&diags, cap) // for standalone dataset resources
```

Builder methods (on `*OpenLineageEventBuilder`):
- `BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent` — assembles the job payload with capability-gated facets.
- `BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent` — wraps `BuildJobEvent`, sets `eventType = COMPLETE`, generates `run.runId` internally.
- `BuildDatasetEvent(data *DatasetResourceModel) *openlineage.DatasetEvent` — assembles a standalone dataset event.

Package-level convenience wrappers (diagnostics are silently discarded — use the builder directly when you need error reporting):

```go
ol.BuildRunEvent(data *JobResourceModel, cap JobCapability) *openlineage.RunEvent
ol.BuildDatasetEvent(data *DatasetResourceModel, cap DatasetCapability) *openlineage.DatasetEvent
```

The builder delegates facet construction to the models themselves via `JobFacetBuilder` and `DatasetFacetBuilder` interfaces (defined in `facet_builders.go`). Each facet model implements the appropriate interface. Disabled facets are never called — the `present bool` guard in the dispatch tables catches nil model pointers before they are boxed into the interface, avoiding the classic nil-pointer-in-interface panic.

Required-field validation is performed by `requireString`: when a required attribute is null or unknown, a path-aware diagnostic error is added to the `Diagnostics` sink and building continues. Use `NewJobEventBuilder(&diags, cap)` directly (not the package-level wrappers) when you need those errors surfaced to Terraform.

Current behavior:

- `BuildJobEvent` builds the shared job payload; only facets enabled in `cap` are emitted,
- `BuildRunEvent` wraps `BuildJobEvent`, sets `eventType = COMPLETE`, generates `run.runId` internally, and does **not** populate run facets,
- `BuildDatasetEvent` builds a standalone dataset event; only facets enabled in `cap` are emitted,
- job identity is built from `namespace` and `name`,
- `inputs` and `outputs` are mapped to OL dataset elements with capability-gated facets,
- dataset events and dataset elements reuse the same dataset-facet mapping helpers,
- disabled facets are skipped even when the corresponding model blocks are populated (portability stub values are ignored).

## Package Files

| File | ~Lines | Contents |
|---|---|---|
| `base_resource.go` | ~190 | `ResourceBackend` interface, `resourceBase[B]` generic struct, all shared CRUD methods |
| `base_job_resource.go` | ~60 | `JobResourceBackend` interface, `BaseJobResource` (Metadata + BaseSchema) |
| `base_dataset_resource.go` | ~40 | `DatasetResourceBackend` interface, `BaseDatasetResource` (Metadata + BaseSchema) |
| `capability.go` | ~155 | `JobFacet`/`DatasetFacet` types, facet constants, `JobCapability`/`DatasetCapability` |
| `schema_generator.go` | ~715 | capability-driven Terraform schema builders for job and dataset resources |
| `facet_builders.go` | ~315 | `JobFacetBuilder` / `DatasetFacetBuilder` interfaces; `Build` methods on every facet model |
| `event_builder.go` | ~235 | `OpenLineageEventBuilder`, constructors, dispatch tables, package-level wrappers, helpers |
| `models.go` | ~35 | top-level Terraform state models (`JobResourceModel`, `DatasetResourceModel`) |
| `ol_models.go` | ~295 | all OpenLineage-shaped nested models (job facets, dataset facets, column lineage) |

Test files: `capability_test.go` (~205 lines), `schema_generator_test.go` (~450 lines), `event_builder_test.go` (~955 lines).

## For Provider Authors

This section is a step-by-step guide to building a Terraform provider that targets a new
OpenLineage consumer (Marquez, Atlas, etc.) using this package as a base.

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

`JobResourceBackend` requires only `Capability()` on top of the seven methods from `ResourceBackend`.

### Step 3 — Declare `Capability`

Start from `EmptyJobCapability()` and enable only what the target system supports.
Facets that are not enabled remain in the schema as `Optional + Computed` stubs —
config copied from another consumer is accepted without error, values are ignored.

Use `WithFacetEnabled` for job-level facets and `WithDatasetFacetEnabled` for facets
that should be emitted on the job's inputs and outputs.

```go
func (r *MyJobResource) Capability() ol.JobCapability {
    return ol.EmptyJobCapability().
        WithFacetEnabled(ol.FacetJobType, ol.FacetJobOwnership).
        WithDatasetFacetEnabled(ol.FacetDatasetSymlinks, ol.FacetDatasetCatalog)
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
func (r *MyJobResource) ConsumerEmit(ctx context.Context, modelAny any) diag.Diagnostics {
    var diags diag.Diagnostics
    model := modelAny.(*MyJobModel)

    event := ol.NewJobEventBuilder(&diags, r.Capability()).BuildRunEvent(&model.JobResourceModel)
    if diags.HasError() {
        return diags
    }
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
to `resourceBase`, which calls `RemoveResource()` to trigger re-create on next plan.

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

### Step 8 — Implement `ConsumerConfigure`

Initialise the consumer client from provider-level configuration data passed by the framework.

```go
func (r *MyJobResource) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
    if req.ProviderData == nil {
        return
    }
    client, ok := req.ProviderData.(*MyClient)
    if !ok {
        resp.Diagnostics.AddError("Unexpected provider data", fmt.Sprintf("expected *MyClient, got %T", req.ProviderData))
        return
    }
    r.client = client
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
| Build fails after package update | consumer still uses old package-level `BuildRunEvent` with mismatched signature | use `ol.NewJobEventBuilder(&diags, r.Capability()).BuildRunEvent(&model.JobResourceModel)` — diagnostics are surfaced and run ID can be read from `event.Run.RunID` |
| Dataset facets missing on job inputs/outputs | dataset facets enabled via `WithFacetEnabled` instead of `WithDatasetFacetEnabled` | use `WithDatasetFacetEnabled(ol.FacetDataset*)` for inputs/outputs facets |
| Compile error passing `FacetJob*` to `DatasetCapability` | job facets used in a dataset capability | `FacetJob*` constants are type `JobFacet` and are only accepted by `JobCapability.WithFacetEnabled` |
| `nil pointer dereference` in `BuildJobFacet` / `BuildDatasetFacet` | storing a nil model pointer in a `JobFacetBuilder` / `DatasetFacetBuilder` interface variable | always use the `present bool` guard before boxing a pointer into the interface — see `buildJobFacets` in `event_builder.go` |
