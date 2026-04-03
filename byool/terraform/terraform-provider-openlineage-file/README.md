# terraform-provider-openlineage-file

A **reference implementation** of an [OpenLineage](https://openlineage.io) Terraform provider
built on the [`byool/terraform`](../openlineage-base-resource) framework.

This provider writes OpenLineage events to local JSON files — no external services,
no credentials, no network access. Its sole purpose is to show provider developers
**exactly how to implement the `byool/terraform` backend interfaces** with the minimum
possible noise, so the framework's patterns are easy to follow and copy into a real backend.

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Quick start](#quick-start)
3. [Local development setup](#local-development-setup)
4. [Resource reference](#resource-reference)
   - [openlineage_run](#openlineage_run)
   - [openlineage_job](#openlineage_job)
   - [openlineage_dataset](#openlineage_dataset)
5. [On-disk file format](#on-disk-file-format)
6. [Provider configuration](#provider-configuration)
7. [Make targets](#make-targets)
8. [Architecture — for provider developers](#architecture--for-provider-developers)
   - [Framework overview](#framework-overview)
   - [Three event types](#three-event-types)
   - [Backend interfaces](#backend-interfaces)
   - [How BaseJobResource works](#how-basejobresource-works)
   - [How BaseDatasetResource works](#how-basedatasetresource-works)
   - [Capability system](#capability-system)
   - [Schema generation and stubs](#schema-generation-and-stubs)
   - [How to implement a new backend](#how-to-implement-a-new-backend)
9. [Project structure](#project-structure)

---

## What it does

`terraform apply` writes one JSON file per resource to `output_dir/`:

```
lineage/
  run__production__my-etl-job.json       ← RunEvent  (openlineage_run)
  job__production__my-pipeline.json      ← JobEvent  (openlineage_job)
  dataset__postgres__mydb.public.orders.json  ← DatasetEvent (openlineage_dataset)
```

Each file contains the full OpenLineage event exactly as it would be sent to a real
backend (Marquez, Dataplex, …), wrapped with provider metadata:

```json
{
  "schema_version": "1.0",
  "job":          { "namespace": "production", "name": "my-etl-job" },
  "last_emitted": "2026-03-31T12:00:00Z",
  "emit_count":   3,
  "run_id":       "550e8400-e29b-41d4-a716-446655440000",
  "event":        { /* full OL RunEvent / JobEvent / DatasetEvent */ }
}
```

`terraform destroy` removes the file.  
If the file is deleted outside Terraform, the next `plan` detects drift and proposes re-create.

---

## Quick start

```bash
# 1. Build the provider binary
make build

# 2. Create .terraformrc pointing Terraform to the local binary (one-time setup)
#    See "Local development setup" below.

# 3. Apply the example configuration
make apply

# 4. Inspect the output
cat terraform-example/lineage/run__production__my-etl-job.json | jq .
```

---

## Local development setup

### Prerequisites

- Go ≥ 1.26
- Terraform ≥ 1.0

### 1. Configure go.work

The provider depends on the `openlineage-base-resource` module which lives at
`../openlineage-base-resource` (a sibling directory). `go.work` redirects the import:

```
go 1.26.0

use .

replace github.com/OpenLineage/openlineage/byool/terraform => ../openlineage-base-resource
```

`go.work` is git-ignored — it is machine-local. Copy from the template if missing:

```bash
cp go.work.example go.work
```

### 2. Create `.terraformrc`

Terraform's `dev_overrides` mechanism lets you run a locally built binary without
publishing to a registry and without running `terraform init`.

Create `.terraformrc` in the project root (it is git-ignored):

```hcl
provider_installation {
  dev_overrides {
    "registry.terraform.io/openlineage/openlineage-file" = "/absolute/path/to/this/repo/bin"
  }
  direct {}
}
```

Replace the path with the absolute path to the `bin/` directory in this repo.

> **Note:** Never run `terraform init` when using `dev_overrides` — it will attempt
> to resolve the provider from the registry and fail. The `make plan` / `make apply`
> targets set `TF_CLI_CONFIG_FILE` automatically.

### 3. Build and apply

```bash
make build    # compile provider binary → bin/terraform-provider-openlineage-file
make plan     # build + terraform plan
make apply    # build + terraform apply -auto-approve
make destroy  # build + terraform destroy -auto-approve
make clean    # remove bin/, lineage files, terraform state
```

---

## Resource reference

### `openlineage_run`

Emits an OpenLineage **RunEvent** — records a specific job execution with a unique
run ID, inputs, and outputs. Use this when you want to capture that a job *ran*
at a point in time and what data it consumed and produced.

```hcl
resource "openlineage_run" "etl" {
  namespace   = "production"
  name        = "my-etl-job"
  description = "Nightly ETL from Postgres to BigQuery"

  job_type {
    processing_type = "BATCH"
    integration     = "SPARK"
    job_type        = "JOB"
  }

  ownership {
    owners {
      name = "team:data-engineering"
      type = "OWNER"
    }
  }

  inputs {
    namespace = "postgres"
    name      = "mydb.public.orders"

    schema {
      fields { name = "id"     type = "INTEGER" }
      fields { name = "amount" type = "NUMERIC" }
    }
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.order_summary"

    column_lineage {
      fields {
        name = "total_amount"
        input_field {
          namespace = "postgres"
          name      = "mydb.public.orders"
          field     = "amount"
          transformation {
            type    = "DIRECT"
            subtype = "AGGREGATION"
            masking = false
          }
        }
      }
    }
  }
}
```

**Computed attributes**

| Attribute | Description |
|-----------|-------------|
| `file_path` | Absolute path to `lineage/run__{namespace}__{name}.json`. Stable across applies. |
| `last_emitted` | RFC3339 UTC timestamp of the most recent write. Updated on every apply. |
| `emit_count` | Number of times this run's lineage has been emitted. Starts at 1. |

**Enabled facets:** `job_type`, `ownership`, `symlinks`, `schema`, `column_lineage`

---

### `openlineage_job`

Emits an OpenLineage **JobEvent** — static job metadata without a run ID. Use this
to register a job's identity, ownership, and typical data flow independently of any
specific execution. Equivalent to "create or update the job definition" in a lineage
catalog.

```hcl
resource "openlineage_job" "pipeline" {
  namespace   = "production"
  name        = "my-pipeline"
  description = "Customer data pipeline"

  job_type {
    processing_type = "BATCH"
    integration     = "AIRFLOW"
    job_type        = "DAG"
  }

  ownership {
    owners {
      name = "team:platform"
      type = "OWNER"
    }
  }

  inputs {
    namespace = "postgres"
    name      = "mydb.public.customers"

    schema {
      fields { name = "id"    type = "INTEGER" }
      fields { name = "email" type = "VARCHAR" }
    }
  }

  outputs {
    namespace = "bigquery"
    name      = "my-project.analytics.customers_clean"
  }
}
```

**Computed attributes** — same as `openlineage_run` (`file_path`, `last_emitted`, `emit_count`).

**Enabled facets:** `job_type`, `ownership`, `symlinks`, `schema`

---

### `openlineage_dataset`

Emits an OpenLineage **DatasetEvent** — standalone dataset metadata independent of
any job. Use this to register a dataset's schema and ownership directly in the
lineage catalog.

```hcl
resource "openlineage_dataset" "orders" {
  namespace = "postgres"
  name      = "mydb.public.orders"

  schema {
    fields { name = "id"         type = "INTEGER" }
    fields { name = "amount"     type = "NUMERIC"    description = "Order amount in USD" }
    fields { name = "created_at" type = "TIMESTAMP" }
  }

  documentation {
    description = "Raw orders table from the production PostgreSQL database."
  }

  ownership {
    owners {
      name = "team:data-engineering"
      type = "OWNER"
    }
  }
}
```

**Computed attributes** — same as `openlineage_run` (`file_path`, `last_emitted`, `emit_count`).

**Enabled facets:** `symlinks`, `schema`, `documentation`, `ownership`

---

## On-disk file format

All three resource types write the same `FileRecord` wrapper around the raw OL event:

```json
{
  "schema_version": "1.0",
  "job": {
    "namespace": "production",
    "name":      "my-etl-job"
  },
  "last_emitted": "2026-03-31T12:00:00Z",
  "emit_count":   1,
  "run_id":       "550e8400-e29b-41d4-a716-446655440000",
  "event": {
    "schemaURL": "https://openlineage.io/spec/...",
    "eventTime": "2026-03-31T12:00:00Z",
    "producer":  "https://github.com/OpenLineage/openlineage/byool/terraform",
    "job":       { ... },
    "run":       { ... },
    "inputs":    [ ... ],
    "outputs":   [ ... ]
  }
}
```

- `run_id` is only populated for `openlineage_run` resources; empty string otherwise.
- `emit_count` is stored in the file so it survives `terraform state rm` + re-import —
  `ConsumerRead()` recovers it by reading the file, demonstrating how a real backend
  reconciles Terraform state with external system state.
- Writes are atomic: data is written to a temp file then renamed into place, preventing
  readers from seeing a partial write.

**File naming:**

```
{output_dir}/{prefix}__{namespace}__{name}.json
```

| Resource type | Prefix |
|---|---|
| `openlineage_run` | `run` |
| `openlineage_job` | `job` |
| `openlineage_dataset` | `dataset` |

The prefix prevents collisions when the same `namespace` + `name` is used across
multiple resource types. Slashes in namespace/name are replaced with dashes.

---

## Provider configuration

```hcl
provider "openlineage" {
  output_dir   = "./lineage"  # required — created automatically if absent
  pretty_print = true         # optional — default true; false for compact JSON
}
```

---

## Make targets

| Target | Description |
|--------|-------------|
| `make build` | Compile → `bin/terraform-provider-openlineage-file` |
| `make plan` | `build` + `terraform plan` with `dev_overrides` |
| `make apply` | `build` + `terraform apply -auto-approve` |
| `make destroy` | `build` + `terraform destroy -auto-approve` |
| `make clean` | Remove `bin/`, `lineage/`, tfstate, `.terraform/` |
| `make test` | `go test ./... -v` |
| `make docs` | Generate provider docs from schema + templates |
| `make docs-install` | Install `tfplugindocs` tool |

---

## Architecture — for provider developers

This section explains the `byool/terraform` framework and how this provider implements
it. Read this if you are building your own OpenLineage Terraform provider.

### Framework overview

```
openlineage-base-resource/ol/
├── base_job_resource.go      BaseJobResource     — generic CRUD for job resources
├── base_dataset_resource.go  BaseDatasetResource — generic CRUD for dataset resources
├── event_builder.go          BuildRunEvent / BuildJobEvent / BuildDatasetEvent
├── schema_generator.go       GenerateJobSchema / GenerateDatasetSchema
├── capability.go             JobCapability / DatasetCapability (opt-in facet system)
├── models.go                 JobResourceModel, DatasetResourceModel
└── ol_models.go              All Terraform tfsdk-tagged facet structs
```

The base module owns **all generic Terraform logic**:
schema generation, CRUD lifecycle, model deserialisation, capability-driven stubbing.  
A concrete provider only implements the **backend interface** — what to do when
an event needs to be emitted, checked, or deleted.

### Three event types

| Builder | Event type | Has run ID | Has inputs/outputs |
|---------|-----------|----------|-------------------|
| `ol.BuildRunEvent(*JobResourceModel)` | `RunEvent` | ✅ generated internally | ✅ |
| `ol.BuildJobEvent(*JobResourceModel)` | `JobEvent` | ❌ | ✅ |
| `ol.BuildDatasetEvent(*DatasetResourceModel)` | `DatasetEvent` | ❌ | ❌ |

All three return a struct with `.AsEmittable()` — a `json.Marshal`-ready representation
of the event in the OL wire format.

### Backend interfaces

**`ol.JobResourceBackend`** — implement to get `openlineage_run` or `openlineage_job`:

```go
type JobResourceBackend interface {
    Capability()        JobCapability
    ConsumerConfigure(ctx, req, resp)
    ConsumerAttributes() map[string]schema.Attribute
    ConsumerBlocks()     map[string]schema.Block
    NewModel()           any
    ConsumerEmit(ctx, model any, runID uuid.UUID) diag.Diagnostics
    ConsumerRead(ctx, model any) (exists bool, diags diag.Diagnostics)
    ConsumerDelete(ctx, model any) diag.Diagnostics
}
```

**`ol.DatasetResourceBackend`** — implement to get `openlineage_dataset`:

```go
type DatasetResourceBackend interface {
    Capability()        DatasetCapability
    ConsumerConfigure(ctx, req, resp)
    ConsumerAttributes() map[string]schema.Attribute
    ConsumerBlocks()     map[string]schema.Block
    NewModel()           any
    ConsumerEmit(ctx, model any, runID uuid.UUID) diag.Diagnostics
    ConsumerRead(ctx, model any) (exists bool, diags diag.Diagnostics)
    ConsumerDelete(ctx, model any) diag.Diagnostics
}
```

### How BaseJobResource works

`BaseJobResource` is embedded in your resource struct via the **self-reference pattern**:

```go
type MyJobResource struct {
    ol.BaseJobResource   // inherits Metadata, Schema, Create, Read, Update, Delete
    myClient *MyClient
}

func NewMyJobResource() resource.Resource {
    r := &MyJobResource{}
    r.Backend = r  // self-reference: this struct IS the backend
    return r
}
```

`BaseJobResource` delegates every operation to `r.Backend`:

- `Schema()` → calls `GenerateJobSchema(Backend.Capability())` then merges in `ConsumerAttributes()` and `ConsumerBlocks()`
- `Create()` / `Update()` → deserialise plan into `Backend.NewModel()`, then call `Backend.ConsumerEmit()`
- `Read()` → deserialise state into `Backend.NewModel()`, call `Backend.ConsumerRead()`. If returns `false`, remove from state (triggers re-create on next plan)
- `Delete()` → deserialise state into `Backend.NewModel()`, call `Backend.ConsumerDelete()`

**Overriding Metadata:** `BaseJobResource.Metadata()` registers the type as `{provider}_job`.
To register as a different name (e.g. `{provider}_run`), override `Metadata()` on your struct:

```go
func (r *MyRunResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
    resp.TypeName = req.ProviderTypeName + "_run"
}
```

### How BaseDatasetResource works

Identical pattern to `BaseJobResource` but for `DatasetResourceBackend` and
`GenerateDatasetSchema`. `Metadata()` registers the type as `{provider}_dataset`.

### Capability system

Capabilities are the **opt-in mechanism** for OL facets. Each facet you enable becomes
a fully functional block in the Terraform schema. Each facet you do **not** enable is
included as a silent no-op stub — all attributes `Optional+Computed`, no validators —
so a `.tf` config written for a richer provider is accepted without error.

```go
// Enable only what your backend actually supports
func (r *MyResource) Capability() ol.JobCapability {
    return ol.EmptyJobCapability().WithFacetEnabled(
        ol.FacetJobType,
        ol.FacetJobOwnership,
        ol.FacetDatasetSchema,
        ol.FacetDatasetColumnLineage,
        // everything else is a stub
    )
}
```

Available job facets: `FacetJobType`, `FacetJobOwnership`, `FacetJobDocumentation`,
`FacetJobSourceCode`, `FacetJobSourceCodeLocation`, `FacetJobSQL`, `FacetJobTags`

Available dataset facets: `FacetDatasetSymlinks`, `FacetDatasetSchema`,
`FacetDatasetDataSource`, `FacetDatasetDocumentation`, `FacetDatasetType`,
`FacetDatasetVersion`, `FacetDatasetStorage`, `FacetDatasetOwnership`,
`FacetDatasetLifecycleStateChange`, `FacetDatasetHierarchy`, `FacetDatasetCatalog`,
`FacetDatasetColumnLineage`, `FacetDatasetTags`

### Schema generation and stubs

`GenerateJobSchema(capability)` builds the full Terraform schema:
- Enabled facets → `Required` / `Optional` attributes as defined in the OL spec
- Disabled facets → every attribute in the block becomes `Optional + Computed`, no validators

This means you can share one `.tf` config across multiple providers (e.g. the file
provider and a Dataplex provider) — facets unsupported by a given provider are
silently accepted and ignored.

### How to implement a new backend

Below is the minimum implementation needed to create a new provider that sends
OL events to a custom backend (e.g. a REST API or message queue).

**1. Define your state model**

```go
type MyJobModel struct {
    ol.JobResourceModel   // promoted: namespace, name, job facets, inputs, outputs
    MyState               // promoted: your computed fields
}

type MyState struct {
    EventID types.String `tfsdk:"event_id"`   // e.g. returned by the API
}
```

**2. Implement the backend**

```go
type MyJobResource struct {
    ol.BaseJobResource
    client *myapi.Client
}

func (r *MyJobResource) Capability() ol.JobCapability {
    return ol.EmptyJobCapability().WithFacetEnabled(
        ol.FacetJobType,
        ol.FacetJobOwnership,
    )
}

func (r *MyJobResource) ConsumerAttributes() map[string]schema.Attribute {
    return map[string]schema.Attribute{
        "event_id": schema.StringAttribute{Computed: true, ...},
    }
}

func (r *MyJobResource) NewModel() any { return &MyJobModel{} }

func (r *MyJobResource) ConsumerEmit(ctx context.Context, modelAny any, _ uuid.UUID) diag.Diagnostics {
    model := modelAny.(*MyJobModel)
    event := ol.BuildRunEvent(&model.JobResourceModel)
    id, err := r.client.Send(event.AsEmittable())
    // ... store id in model.EventID
}

func (r *MyJobResource) ConsumerRead(ctx context.Context, modelAny any) (bool, diag.Diagnostics) {
    model := modelAny.(*MyJobModel)
    exists, err := r.client.Exists(model.EventID.ValueString())
    return exists, diags
}

func (r *MyJobResource) ConsumerDelete(ctx context.Context, modelAny any) diag.Diagnostics {
    model := modelAny.(*MyJobModel)
    err := r.client.Delete(model.EventID.ValueString())
    // ...
}
```

**3. Wire it up**

```go
func NewMyJobResource() resource.Resource {
    r := &MyJobResource{}
    r.Backend = r
    return r
}

// In provider.go:
func (p *MyProvider) Resources(_ context.Context) []func() resource.Resource {
    return []func() resource.Resource{
        NewMyJobResource,
    }
}
```

**Method mapping — this provider vs a real backend**

| Method | This provider (file) | Real backend example (Dataplex) |
|--------|----------------------|---------------------------------|
| `Capability()` | Enable a subset of facets | Enable only facets the API supports |
| `ConsumerEmit()` | `os.WriteFile` | `ProcessOpenLineageRunEvent` (gRPC) |
| `ConsumerRead()` | `os.Stat` + read JSON | `GetProcess` (gRPC) — return `false` on 404 |
| `ConsumerDelete()` | `os.Remove` | `DeleteProcess` (gRPC) |
| `ConsumerConfigure()` | Store `output_dir` | Store gRPC client from provider config |

---

## Project structure

```
terraform-provider-openlineage-file/
├── main.go                            Provider entry point
├── go.mod                             module github.com/…/terraform-example-file
├── go.work                            Local dev: replace base module with ../openlineage-base-resource
├── go.work.example                    Template for go.work
├── .terraformrc                       Local dev_overrides (git-ignored)
├── Makefile
├── README.md
├── CONTEXT.md                         Session context / quick reference
├── terraform-registry-manifest.json
│
├── internal/file/
│   ├── provider.go                    OpenLineageProvider — registers all three resources
│   ├── file_models.go                 FileRunModel, FileJobModel, FileDatasetModel, FileState
│   ├── file_run_resource.go           openlineage_run     — RunEvent, overrides Metadata to _run
│   ├── file_job_resource.go           openlineage_job     — JobEvent, default _job Metadata
│   ├── file_dataset_resource.go       openlineage_dataset — DatasetEvent, BaseDatasetResource
│   └── file_writer.go                 FilePath() / Write() / Read() / Delete()
│
├── terraform-example/
│   └── main.tf                        Example config for all three resource types
│
├── examples/
│   ├── provider/provider.tf
│   └── resources/openlineage_job/resource.tf
│
└── templates/                         tfplugindocs templates
    ├── index.md.tmpl
    └── resources/job.md.tmpl
```
