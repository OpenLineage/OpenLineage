# terraform-provider-openlineage-file — Session Context

_Last updated: 2026-03-31_

---

## What this provider does

A local-file reference implementation of an OpenLineage Terraform provider built on
`../openlineage-base-resource` (`github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource`).

On `terraform apply` it writes one JSON file per resource to `output_dir/`.  
On `terraform destroy` it removes those files.  
No external services, no credentials, no network.

---

## Module layout

```
terraform-provider-openlineage-file/
├── main.go
├── go.mod          module github.com/OpenLineage/openlineage/byool/terraform-example-file
├── go.work         local dev — replace byool/terraform => ../openlineage-base-resource
├── .terraformrc    dev_overrides for local Terraform testing (git-ignored)
├── Makefile        build / plan / apply / destroy / clean / test / docs
├── internal/file/
│   ├── provider.go              OpenLineageProvider — registers all three resources
│   ├── file_models.go           FileRunModel, FileJobModel, FileDatasetModel, FileState
│   ├── file_run_resource.go     openlineage_run     — emits RunEvent
│   ├── file_job_resource.go     openlineage_job     — emits JobEvent
│   ├── file_dataset_resource.go openlineage_dataset — emits DatasetEvent
│   └── file_writer.go           FilePath / Write / Read / Delete helpers
└── terraform-example/
    └── main.tf                  example config for all three resource types
```

---

## Three resource types

| Terraform resource    | OL event type  | Output file pattern                        | Base struct             |
|-----------------------|----------------|--------------------------------------------|-------------------------|
| `openlineage_run`     | `RunEvent`     | `lineage/run__{namespace}__{name}.json`    | `ol.BaseJobResource`    |
| `openlineage_job`     | `JobEvent`     | `lineage/job__{namespace}__{name}.json`    | `ol.BaseJobResource`    |
| `openlineage_dataset` | `DatasetEvent` | `lineage/dataset__{namespace}__{name}.json`| `ol.BaseDatasetResource`|

### openlineage_run
- Emits a `RunEvent` (job execution with unique run ID, inputs, outputs).
- Overrides `Metadata()` to register as `_run` (base default is `_job`).
- Uses `ol.BuildRunEvent(&model.JobResourceModel)` — run ID generated internally.
- Enabled facets: `FacetJobType`, `FacetJobOwnership`, `FacetDatasetSymlinks`,
  `FacetDatasetSchema`, `FacetDatasetColumnLineage`.

### openlineage_job
- Emits a `JobEvent` (static job metadata, no run ID).
- Uses default `Metadata()` from `ol.BaseJobResource` → registers as `_job`.
- Uses `ol.BuildJobEvent(&model.JobResourceModel)`.
- Enabled facets: `FacetJobType`, `FacetJobOwnership`, `FacetDatasetSymlinks`,
  `FacetDatasetSchema`.

### openlineage_dataset
- Emits a `DatasetEvent` (standalone dataset metadata, no job context).
- Uses `ol.BaseDatasetResource` → registers as `_dataset`.
- Uses `ol.BuildDatasetEvent(&model.DatasetResourceModel)`.
- Enabled facets: `FacetDatasetSymlinks`, `FacetDatasetSchema`,
  `FacetDatasetDocumentation`, `FacetDatasetOwnership`.

---

## Shared computed attributes (all three resources)

| Attribute     | Description                                              |
|---------------|----------------------------------------------------------|
| `file_path`   | Stable absolute path to the JSON file. `UseStateForUnknown`. |
| `last_emitted`| RFC3339 UTC timestamp, updated on every apply.           |
| `emit_count`  | Increments on every apply. Recovered from file on drift. |

---

## go.work (local dev)

```
go 1.26.0

use .

replace github.com/OpenLineage/openlineage/byool/terraform => ../openlineage-base-resource
```

The base module is the sibling directory `../openlineage-base-resource`
(`module github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource`).

---

## .terraformrc (local dev, git-ignored)

```hcl
provider_installation {
  dev_overrides {
    "registry.terraform.io/openlineage/openlineage-file" = "/absolute/path/to/bin"
  }
  direct {}
}
```

- No `terraform init` needed when `dev_overrides` is active.
- No `.terraform.lock.hcl` needed — do not hand-edit it.

---

## Key base module API (openlineage-base-resource/ol)

```go
// Event builders
ol.BuildRunEvent(data *JobResourceModel) *openlineage.RunEvent
ol.BuildJobEvent(data *JobResourceModel) *openlineage.JobEvent
ol.BuildDatasetEvent(data *DatasetResourceModel) *openlineage.DatasetEvent

// All three have: event.AsEmittable() → json.Marshal-ready

// Models (no id / run_id in Terraform state)
type JobResourceModel struct {
    OLJobConfig             // namespace, name, description, job_type, ownership, …
    Inputs  []OLInputModel
    Outputs []OLOutputModel
}
type DatasetResourceModel struct {
    DatasetModel            // namespace, name, symlinks, schema, catalog, …
}

// Capability builders
ol.EmptyJobCapability().WithFacetEnabled(...)
ol.EmptyDatasetCapability().WithFacetEnabled(...)
```

---

## FilePath convention

```go
FilePath(outputDir, prefix, namespace, name string) string
// → {outputDir}/{prefix}__{namespace}__{name}.json
// prefix: "run" | "job" | "dataset"
```

The prefix prevents file collisions when the same namespace+name is used
across different resource types.

---

## FileRecord (on-disk format)

```json
{
  "schema_version": "1.0",
  "job":          { "namespace": "...", "name": "..." },
  "last_emitted": "2026-03-31T12:00:00Z",
  "emit_count":   1,
  "run_id":       "uuid-only-for-run-events",
  "event":        { /* full OL event JSON */ }
}
```

`run_id` is populated only for `openlineage_run`; empty string for job/dataset events.

---

## make targets

| Target        | Description                                      |
|---------------|--------------------------------------------------|
| `make build`  | Compile → `bin/terraform-provider-openlineage-file` |
| `make plan`   | build + `terraform plan` with dev_overrides      |
| `make apply`  | build + `terraform apply -auto-approve`          |
| `make destroy`| build + `terraform destroy -auto-approve`        |
| `make clean`  | Remove bin/, lineage/, tfstate, .terraform/      |
| `make test`   | `go test ./...`                                  |

