# Versioning
## Context
The OpenLineage spec gets versioned and published.

Requirements:
- The OpenLineage spec and related libraries are in the OpenLineage repo.
- The OpenLineage spec version changes only when the spec itself changes.
- The libraries in the repo change more frequently than the spec (including when the spec changes).
- We want to version the OpenLineage spec independently of the api spec.
- The mechanism to version and publish the OpenLineage core spec apply to publishing custom facets.

## Mechanism
- The spec defines it’s current version using the `“$id”` field:
  - See:
    - [json schema core doc](https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.8.2.1)
    - [Json schema spec $id](https://json-schema.org/draft/2019-09/schema)
    - [also on github](https://github.com/json-schema-org/json-schema-spec/blob/draft-next/meta/core.json)
  - Example:
`"$id": "https://openlineage.io/spec/1-0-0/OpenLineage.json"`
The URL in $id is resolvable and returns that version of the spec.
We use github pages to publish the spec to openlineage.io
- The $id urls uses a SEMVER compliant version, following the [SCHEMAVER semantics](https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/iglu/common-architecture/schemaver/)
MODEL-REVISION-ADDITION
  - MODEL when you make a breaking schema change which will prevent interaction with any historical data
  - REVISION when you introduce a schema change which may prevent interaction with some historical data
  - ADDITION when you make a schema change that is compatible with all historical data

## Implementation plan
- CI verifies that:
  - the $id field has the right domain prefix
  - the version changes when the spec changed: When resolving “$id”, the build fails if the spec is not exactly the same.
  - The version does not change when the spec does not change. We can verify that the current version of the spec is not already published with a different version.
  - Libraries are generating event with current version
  - Make sure the spec is backward compatible (only add optional fields) and consistent with the versioning semantics
- git pre commit: Increments the versions automatically when the spec changes.
- spec publication:
  - CI publishes to github pages when the $id changes on main (when this particular url does not exist yet)
  - CI tags main with OpenLineage.json-{version}

