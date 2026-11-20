# Example OpenLineage file provider

A small Terraform provider built on the [OpenLineage base resource](../openlineage-base-resource). It shows what a
consumer provider has to implement, and it's what the base resource is tested against end to end.

Instead of sending events to a backend, it writes the latest OpenLineage event of each resource to a JSON file:

| Resource              | Event        | File                                           |
|-----------------------|--------------|------------------------------------------------|
| `openlineage_job`     | JobEvent     | `<output_dir>/jobs/<namespace>/<name>.json`     |
| `openlineage_dataset` | DatasetEvent | `<output_dir>/datasets/<namespace>/<name>.json` |

Namespace and name are URL path-escaped, so `s3://warehouse` becomes the directory `s3:%2F%2Fwarehouse`. Each resource
exports the path as the computed `file_path`. Destroying a resource deletes its file; a missing file is re-created on
the next apply.

It is an example: it is not published to the Terraform registry.

## Provider configuration

```hcl
provider "openlineage" {
  output_dir     = "./out"                           # required
  producer       = "https://example.com/my-pipeline" # optional; defaults to this provider's source URL
  job_event_type = "run"                             # optional: "job" (default) or "run"
}
```

`job_event_type = "run"` writes job events as `COMPLETE` RunEvents with a new run ID on every apply, for consumers that
only accept run events. Those RunEvents are a wrapper, not real runs.

## Supported facets

Every facet the base resource knows, except `source_code` (job) and `catalog` / `fabric_one_lake` (datasets). They are
disabled on purpose: [`examples/main.tf`](examples/main.tf) still uses them, and Terraform accepts and stores them, but
they are not written to the event files. That is how a config written for one provider keeps working with another
that supports fewer facets. The provider doesn't warn about ignored facets.

The facet set is `JobCapability()` / `DatasetCapability()` in [`internal/provider`](internal/provider).

## Run it

```sh
cd byool/terraform/terraform-provider-openlineage-file
go build -o bin/terraform-provider-openlineage .
```

Point Terraform at the binary with a CLI config (here a separate file, so your `~/.terraformrc` stays untouched):

```sh
cat > /tmp/openlineage-dev.tfrc <<EOF
provider_installation {
  dev_overrides {
    "openlineage/openlineage" = "$PWD/bin"
  }
  direct {}
}
EOF
export TF_CLI_CONFIG_FILE=/tmp/openlineage-dev.tfrc

cd examples
terraform plan      # no `terraform init` with dev_overrides
terraform apply
cat out/jobs/example/orders_daily.json
terraform destroy
```

## Code

| File                                 | Contents                                                              |
|--------------------------------------|-----------------------------------------------------------------------|
| `main.go`                            | `providerserver.Serve`                                                |
| `internal/provider/provider.go`      | provider schema and config; registers the base job and dataset resources |
| `internal/provider/job_backend.go`   | `ol.JobResourceBackend`: capability, event building, file per job     |
| `internal/provider/dataset_backend.go` | `ol.DatasetResourceBackend`: the same for datasets                  |
| `internal/provider/files.go`         | shared backend parts: provider config, `file_path`, file I/O          |

## Tests

```sh
TF_ACC_TERRAFORM_VERSION=1.13.4 go test ./...
```

`provider_test.go` applies `examples/main.tf` with real `terraform` into a temporary directory, then checks the
written events (enabled facets present, disabled ones absent), updates, `job_event_type = "run"`, and that destroy
removes the files.
