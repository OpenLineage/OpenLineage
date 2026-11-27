# OpenLineage Terraform base resource (`ol` package)

Generic building blocks for Terraform providers that send OpenLineage metadata to a consumer (Marquez, a catalog, a
file, …). The package owns everything OpenLineage-shaped — schema, state models, validation and event building for
every job and dataset facet in the spec — so a provider only implements how events reach its consumer.

Most of the package is generated from the spec by [`generator/go`](../../../generator/go). A spec change is picked up by
regenerating; new facets need no hand-written code.

See [`../terraform-provider-openlineage-file`](../terraform-provider-openlineage-file) for a complete example provider.

## How it fits together

```
provider (yours)
  Resources(): ol.NewJobResource(backend), ol.NewDatasetResource(backend)
                     │
                     ▼
ol job / dataset resource  ── schema from GenerateJobSchema / GenerateDatasetSchema(backend.Capability())
                              + backend.ConsumerAttributes() / ConsumerBlocks()
                           ── CRUD: plan → model → backend.ConsumerEmit / ConsumerRead / ConsumerDelete
```

A provider implements `ol.JobResourceBackend` and/or `ol.DatasetResourceBackend` and passes it to the constructor. The
resources are named `<provider>_job` and `<provider>_dataset`.

| Method                   | Purpose                                                                                     |
|--------------------------|---------------------------------------------------------------------------------------------|
| `Capability()`           | which facets this consumer supports (`ol.JobCapability` / `ol.DatasetCapability`)           |
| `ConsumerConfigure(...)` | picks up provider-level config (`req.ProviderData`)                                         |
| `ConsumerAttributes()`   | extra top-level attributes (e.g. a computed ID)                                             |
| `ConsumerBlocks()`       | extra top-level blocks                                                                      |
| `NewModel()`             | pointer to a fresh state struct (embeds `ol.JobResourceModel` / `ol.DatasetResourceModel`)  |
| `ConsumerEmit(...)`      | builds the event from the model and sends it; may set computed attributes on the model     |
| `ConsumerRead(...)`      | reports whether the entity still exists (`false` → Terraform re-creates it)                 |
| `ConsumerDelete(...)`    | removes the entity (treat "not found" as success)                                           |

Create and Update both call `ConsumerEmit`; the event describes the resource as configured.

## Writing a provider

### 1. State model

Embed the base model without a `tfsdk` tag (the framework promotes embedded fields) and add your own attributes:

```go
type myJobModel struct {
	ol.JobResourceModel
	RemoteID types.String `tfsdk:"remote_id"`
}
```

Every attribute and block of the schema needs a field, including the ones from `ConsumerAttributes()`.

### 2. Backend

```go
type myJobBackend struct {
	client *myClient
}

var _ ol.JobResourceBackend = (*myJobBackend)(nil)

func (b *myJobBackend) Capability() ol.JobCapability {
	return ol.EmptyJobCapability().
		WithFacetEnabled(ol.JobTypeJobFacet, ol.OwnershipJobFacet, ol.SQLJobFacet).
		WithDatasetFacetEnabled(ol.SchemaDatasetFacet, ol.SymlinksDatasetFacet, ol.ColumnLineageDatasetFacet)
}

func (b *myJobBackend) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return // not configured yet (validation)
	}
	client, ok := req.ProviderData.(*myClient)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", fmt.Sprintf("got %T", req.ProviderData))
		return
	}
	b.client = client
}

func (b *myJobBackend) ConsumerAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"remote_id": schema.StringAttribute{
			Computed:      true,
			PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
		},
	}
}

func (b *myJobBackend) ConsumerBlocks() map[string]schema.Block { return nil }

func (b *myJobBackend) NewModel() any { return &myJobModel{} }

func (b *myJobBackend) ConsumerEmit(ctx context.Context, model any) diag.Diagnostics {
	m := model.(*myJobModel)
	var diags diag.Diagnostics
	event := ol.NewJobEventBuilder(&diags, b.Capability(), producer).BuildJobEvent(&m.JobResourceModel)
	if diags.HasError() {
		return diags
	}
	id, err := b.client.Send(ctx, event)
	if err != nil {
		diags.AddError("Send event", err.Error())
		return diags
	}
	m.RemoteID = types.StringValue(id)
	return diags
}

// ConsumerRead and ConsumerDelete: see the table above.
```

### 3. Provider

```go
func (p *myProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		func() resource.Resource { return ol.NewJobResource(&myJobBackend{}) },
		func() resource.Resource { return ol.NewDatasetResource(&myDatasetBackend{}) },
	}
}
```

The constructors panic on a nil backend. Return a new backend per call: the framework creates a resource instance per
operation and calls `Configure` on it.

### Producer

`producer` is the third argument of `NewJobEventBuilder` / `NewDatasetEventBuilder` and is written to every event and
facet. Use your provider's own URI (for example its source repository), not the base module's.

## Capabilities and disabled facets

`EmptyJobCapability()` / `EmptyDatasetCapability()` enable nothing; `WithFacetEnabled(...)` enables job facets (or
dataset facets on a `DatasetCapability`), and `JobCapability.WithDatasetFacetEnabled(...)` enables dataset facets on a
job's inputs and outputs. Facet constants are generated in `capability.gen.go` (`ol.SQLJobFacet`,
`ol.SchemaDatasetFacet`, …); `JobFacet` and `DatasetFacet` are distinct types, so mixing them up doesn't compile.
`ColumnLineageDatasetFacet` applies to job outputs only.

A facet the consumer doesn't enable keeps its block in the schema, so a config written for another consumer still
works. Its values are **accepted, stored in state as written, and not emitted** — silently, without a warning. All its
attributes are optional and unvalidated. They are not `Computed`: Terraform requires state to match config for
configured values, and `Computed` would only leave unknown values behind for unset ones.

## HCL shape

```hcl
resource "myprovider_job" "orders" {
  namespace = "example"        # namespace / name: required; changing them replaces the resource
  name      = "orders_daily"

  sql {                        # each facet is a block named by its short key
    query = "SELECT ..."
  }

  lineage {
    entries {                  # union list: exactly one variant block per element
      dataset {
        namespace = "s3://warehouse"
        name      = "orders"
        fields {               # map of objects: a list of entries keyed by `name`
          name = "id"
          inputs {
            dataset {
              namespace = "s3://raw"
              name      = "events"
            }
          }
        }
      }
    }
  }

  inputs {                     # datasets carry the dataset facets
    namespace = "s3://raw"
    name      = "events"
  }

  outputs {
    namespace = "s3://warehouse"
    name      = "orders"
    column_lineage { ... }     # outputs only
  }
}
```

- Facet keys are the facet name without the `Facet`/`Job`/`Dataset` suffixes in snake_case (`job_type`,
  `source_code_location`, `symlinks`); `DatasetVersionDatasetFacet` is `version`.
- Attributes the spec requires are enforced when their block is present. Optional list blocks can be left out; lists
  the spec requires need at least one element.
- Unions (lineage `entries` / `inputs`) take one block per variant (`dataset {}` or `job {}`); the spec's `type`
  discriminator is implied.
- Maps of objects become lists of entries with a `name` attribute holding the key; duplicate names are an error.
- Recursive types are unrolled to a fixed depth: schema `fields` can be nested three levels deep.
- Fields HCL can't express are left out, listed in `tfshape.ExcludedFields` in the generator (currently the parent
  job's forwarded `facets` in `parent`).
- `dependentRequired` from the spec is enforced (e.g. a lineage job reference needs both `namespace` and `name`, or
  neither).

## Events

```go
builder := ol.NewJobEventBuilder(&diags, capability, producer)  // job resources
builder.BuildJobEvent(&model.JobResourceModel)                  // *openlineage.JobEvent
builder.BuildRunEvent(&model.JobResourceModel)                  // *openlineage.RunEvent
ol.NewDatasetEventBuilder(&diags, capability, producer).
	BuildDatasetEvent(&model.DatasetResourceModel)              // *openlineage.DatasetEvent
```

Missing required values are reported to `diags` with the attribute path; check `diags.HasError()` before sending. The
package-level wrappers `ol.BuildJobEvent`, `ol.BuildRunEvent` and `ol.BuildDatasetEvent` take the same arguments and
return `(event, diag.Diagnostics)`.

Only facets enabled in the capability are emitted. `BuildRunEvent` wraps the job event in a `COMPLETE` RunEvent with a
new run ID on every call. It exists for consumers whose APIs accept only run events; it does not describe a real run.

## Files

Hand-written:

| File                       | Contents                                                                              |
|----------------------------|---------------------------------------------------------------------------------------|
| `base_resource.go`         | `ResourceBackend`, shared Configure/CRUD                                              |
| `base_job_resource.go`     | `JobResourceBackend`, `NewJobResource`                                                |
| `base_dataset_resource.go` | `DatasetResourceBackend`, `NewDatasetResource`                                        |
| `generator.go`             | `GenerateJobSchema`, `GenerateDatasetSchema`, disabled-facet stubs, schema validators |
| `event_builder.go`         | `OpenLineageEventBuilder`, wrappers, value helpers                                    |

Generated (never edit by hand; change the templates in `generator/go/render/terraform` and regenerate):

| File                              | Contents                                                                  |
|-----------------------------------|---------------------------------------------------------------------------|
| `capability.gen.go`               | facet constants, `JobCapability` / `DatasetCapability`                    |
| `models.gen.go`                   | one model struct per facet and nested type                                |
| `resource_models.gen.go`          | `JobResourceModel`, `DatasetResourceModel`, `OLJobConfig`, `DatasetModel` |
| `facet_schema_descriptors.gen.go` | schema block per facet, `JobFacetSchemas` / `DatasetFacetSchemas`         |
| `facet_builders.gen.go`           | facet dispatch tables                                                     |
| `facet_build_methods.gen.go`      | model → OpenLineage facet conversion                                      |

Regenerate from the repository root:

```sh
cd generator/go && go run . --target terraform
```

CI fails if the committed files differ from the generator's output.

## Development

```sh
./init-workspace.sh   # go.work with this module, the example provider and ../../client/go
go build ./... && go vet ./...
TF_ACC_TERRAFORM_VERSION=1.13.4 go test ./...
```

The tests need a Terraform binary: `TF_ACC_TERRAFORM_VERSION` downloads that version, `TF_ACC_TERRAFORM_PATH` uses a
local one. They cover every generated facet without per-facet code:

- `schema_test.go` — schemas pass the framework's own validation with facets on, off and one at a time; nested
  attributes are never `Required`/`Computed`; list size validators match the spec; a full config round-trips through
  the models.
- `conformance_test.go` — a config filling every facet (every union variant) builds events that validate against the
  spec's JSON schemas.
- `provider_test.go` — real `terraform` plan/apply/destroy against an in-memory provider: all facets on and off, union
  variants, omitted optional lists, `dependentRequired`, nested schema fields, dataset rename, duplicate map entries.
- `event_builder_test.go` — diagnostics and their paths, RunEvent wrapping.

## Common mistakes

| Symptom                                           | Cause                                                          | Fix                                                    |
|---------------------------------------------------|----------------------------------------------------------------|--------------------------------------------------------|
| `invalid tfsdk tag`                               | the embedded `ol.JobResourceModel` has a `tfsdk` tag          | embed it without a tag                                 |
| "struct defines fields not found in object"       | the model lacks a field for a `ConsumerAttributes()` attribute | add the field                                          |
| Dataset facets missing on job inputs/outputs      | enabled with `WithFacetEnabled` on the job capability          | use `WithDatasetFacetEnabled`                          |
| Facets silently missing from events               | the facet isn't enabled in `Capability()`                      | enable it; disabled facets are accepted but not emitted |
| Consumer keeps the old entity after a rename      | `ConsumerDelete` doesn't delete                                | renames replace the resource: delete in `ConsumerDelete` |
