---
sidebar_position: 7
---

# Working with Schemas

OpenLineage is a rapidly growing open source project, and therefore, will face many new changes in its `SPEC`. The spec file is based on [JSON schema specification](https://json-schema.org/) and defines how the OpenLineage's event message would be structured. More details on what are defined in its object model can be found [here](./object-model.md).

When you are working in the OpenLineage project and decided to introduce a new facet or make changes to existing facets, you have to know what needs to be done and also understand how the general build and test process works, so that the OpenLineage specs are well maintained and does not break anything.

The following guidelines may help you to correctly introduce new changes.

## Create a new issue with label `spec`
Before you decide to make any changes, it is best advised that you first label your issue with `spec`. This will indicate the the issue is related to any changes in the current OpenLineage spec.

## Make changes to the spec's version
[Versioning](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Versioning.md) occurs on a per-file basis. Any new spec files start at 1-0-0. Whenever there is a change to existing spec files (JSON), you need to bump up the version of the existing current spec, so that the changes can go through the code generation and gradle build. Consider the following spec file, where you will see the URL in `$id` that shows what is the current spec version the file currently is.

```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
  "$defs": {
```

In this example, bumping up the version to the new value, should be changed from 1-0-1 to 1-0-2.

```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json",
  "$defs": {
```

> If you do not bump the version to higher number, the code generation of Java client will fail.

## Adding and Updating the Schema

Both Python and Java clients automatically generate code to handle the schema, so there is generally little work to do for modifications and new facets. Core logic changes may require manual code in both the Java and Python clients. These changes are rare and require additional planning in the proposal to plan out the steps. These are the steps for adding a new facet, which covers the majority of schema changes.  

> It is important to have prek installed by running `prek install` before committing to the repository. All commits should be signed off with -s `git commit -s -m "commit message"`

> The OpenLineage commutity is very helpful. Do not hesitate to reach out to [#dev-discuss](https://openlineage.slack.com/archives/C065PQ4TL8K) with questions. 

Make your changes

1. Create the facet in `/spec/facets/` (Core spec changes go in `/spec/OpenLineage.json`) 
1. Create an example JSON representation of the facet in `/spec/facets/tests/` 

Configure Java client

1. cd `/client/java`
1. `./gradlew clean publishToMavenLocal` (Publish code to the local Maven project.)
1. `./gradlew generateCode` (Generate the Java classes for new schema changes.)
1. `./gradlew test` (Ensure things are working)

Configure Python client

1. `cd client/python`
1. Update `/client/python/redact_fields.yml` to set any fields that need redaction. (Usually set redact_fields: [])
1. `pip install -r pyproject.toml --extras test --extras msk-iam --extras kafka` (Install dependencies)
1. `pytest` (Ensure tests run. DeprecationWarnings are OK. If any errors occur, check on [#dev-discuss](https://openlineage.slack.com/archives/C065PQ4TL8K))

Commit your code to run Python code generation, various tests, and update website docs. 

1. Optional `prek run` (See if your commit will work.)
1. `git commit -s -m "commit message"` (If anything goes wrong, verify your code.)

## Add test cases (For spec changes that require manual client code.)
Some spec changes require logic changes in the client. See [this PR](https://github.com/OpenLineage/OpenLineage/pull/3186/files#diff-0f689ced46667a2b465edd8311bc217da3ad752877a3515a092b3d46273cb190) that automatically adds an environment variable facet to run events. These types of changes require additional tests. Simply adding or modifying facets do not require new tests. When changing core logic, make sure to add changes to the unit tests for [python](https://github.com/OpenLineage/OpenLineage/tree/main/client/python/tests) and [java](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/test/java/io/openlineage/client) to make sure the unit test can be performed against your new SPEC changes. Refer to existing test codes to add yours in.

