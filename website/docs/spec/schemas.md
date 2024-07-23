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
Whenever there is a change to existing spec file (JSON), you need to bump up the version of the existing current spec, so that the changes can go through the code generation and gradle build. Consider the following spec file, where you will see the URL in `$id` that shows what is the current spec version the file currently is.

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

## Python client's codes need to be manually updated
Java client's build process does involve `code generation` that automatically produces OpenLineage classes derived from the spec files, so you do not need to do anything in terms of coding the client. However, python client libraries does not depend on the spec files to be generated, so you have to make sure to add changes to the python code in order for it to know and use the changes. As for the facets, they are implemented [here](https://github.com/OpenLineage/OpenLineage/blob/main/client/python/openlineage/client/facet.py), so generally, you need to apply necessary changes to it. As for the general structure of OpenLineage's run events, it can be found [here](https://github.com/OpenLineage/OpenLineage/blob/main/client/python/openlineage/client/run.py).

## Add test cases
Make sure to add changes to the unit tests for [python](https://github.com/OpenLineage/OpenLineage/tree/main/client/python/tests) and [java](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/test/java/io/openlineage/client) to make sure the unit test can be performed against your new SPEC changes. Refer to existing test codes to add yours in.

## Test the SPEC change using code generation and integration tests
When you have modified the SPEC file(s), always make sure to perform code generation and unit tests by going into `client/java` and running `./gradlew generateCode` and `./gradlew test`. As for python, cd into `client/python` and run `pytest`.

> Note: Some of the tests may fail due to the fact that they require external systems like kafka. You can ignore those errors.

