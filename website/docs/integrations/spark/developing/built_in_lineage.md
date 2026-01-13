---
sidebar_position: 2
title: Integrating with Spark extensions
---

:::info
Feature available since 1.18.
:::

:::info
To get even better lineage coverage for Spark extensions, we recommend implementing lineage extraction
directly within the extensions' code and this page contains documentation on that.
:::

The Spark ecosystem includes many extensions that affect lineage extraction logic. To facilitate better integration, we
have split our interfaces into two packages: `spark-extension-interfaces` and `spark-extension-entrypoint`.

In general, a mechanism works in a following way:
* Package `spark-extension-interfaces` is a simple and lightweight.
  Its only purpose is to contain methods to generate OpenLineage model objects (like facets, datasets) programmatically
  and interfaces' definitions (Scala traits) to expose lineage information from nodes of Spark logical plan.
* Any extension that adds custom node to Spark logical plan can implement the interfaces.
* Spark OpenLineage integration, when traversing logical plan tree, checks if its nodes implement
  those interfaces and uses their methods to extract lineage metadata from those nodes.

## Problem Definition

The OpenLineage Spark integration relies on the `openlineage-spark-*.jar` library attached to a running Spark job. The
library traverses the Spark logical plan during run state updates to generate OpenLineage events. While traversing the
plan's tree, the library extracts input and output datasets as well as other relevant aspects of the job, run, or
datasets involved in the processing. The extraction code for each node is contained within the `openlineage-spark-*.jar`.

Two main issues with this approach are:

* The Spark ecosystem includes many extensions, many of which add custom nodes to the logical plan of the executed
  query. These nodes need to be traversed and understood by `openlineage-spark` to extract lineage from them. This
  introduces significant complexity to the codebase, as OpenLineage must cover multiple Spark versions and support
  multiple versions of various extensions.
* Spark extensions contain valuable metadata that can be published within OpenLineage events. It makes sense to allow
  extensions to publish facets on their own. This issue contains a great example of useful aspects that can be retrieved
  from the Iceberg extension.

## Solution

A remedy to the problems above is to migrate lineage extraction logic directly to the Spark LogicalPlan nodes. The
advantages of this approach are:

* One-to-one version matching: There is no further need for a single integration code to support multiple versions of a
  Spark extension.
* Avoid breaking changes: This approach limits the number of upgrades that break the integration between
  `openlineage-spark` and other extensions, as lineage extraction code is directly put into the extensions' codebase,
  ensuring that changes on the Spark extension side do not break the integration.

The `spark-extension-interfaces` package contains interfaces that should be implemented as well as utility classes to integrate OpenLineage within any Spark extension.

The `spark-extension-entrypoint` package acts as a bridge to ensure that `openlineage-spark` can identify the shaded
resources of `spark-extension-interfaces` and correctly extract the lineage data.

Package code should not be shipped with the extension that implements the interfaces. The dependency should be marked as
compile-only. The implementation of the code calling the methods should be responsible for providing
`spark-extension-interfaces` on the classpath.

Please note that this package and the interfaces should be considered experimental and may evolve in the future. All the
current logic has been placed in the `*.v1` package. First, it is possible to develop the same interfaces in Java.
Secondly, in case of non-compatible changes, we will release `v2` interfaces. We aim to support different versions
within the Spark integration.

## Extracting lineage from plan nodes

### The easy way - return all the metadata about dataset

Spark optimized logical plan is a tree created of `LogicalPlan` nodes. Oftentimes, it is a Spark extension
internal class that implements `LogicalPlan` and becomes node within a tree. In this case, it is
reasonable to implement lineage extraction logic directly within that class.

Two interfaces have been prepared:
* `io.openlineage.spark.builtin.scala.v1.InputLineageNode` with `getInputs` method,
* `io.openlineage.spark.builtin.scala.v1.OutputLineageNode` with `getOutputs` method.

They return list of `InputDatasetWithFacets` and `OutputDatasetWithFacets` respectively. Each trait has methods
to expose dataset facets as well facets that relate to particular dataset only in the context of
current run, like amount of bytes read from a certain dataset.

### When extracting dataset name and namespace is non-trivial

The simple approach is to let the extension provide dataset identifier containing `namespace` as `name`.
However, in some cases this can be cumbersome.
For example, within Spark's codebase there are several nodes whose output dataset is
`DatasourceV2Relation` and extracting dataset's `name` and `namespace` from such nodes includes
non-trivial logic. In such scenarios, it does not make sense to require an extension to re-implement
the logic already present within `spark-openlineage` code. To solve this, the traits introduce datasets
with delegates which don't contain exact dataset identifier with name and namespace. Instead, they contain
pointer to other member of the plan where `spark-openlineage` should extract identifier from.

For this scenario, case classes `InputDatasetWithDelegate` and
`OutputDatasetWithDelegate` have been created. They allow assigning facets to a dataset, while
still letting other code to extract metadata for the same dataset. The classes contain `node` object
property which defines node within logical plan to contain more metadata about the dataset.
In other words, returning a delegate will make OpenLineage Spark integration extract lineage from
the delegate and enrich it with facets attached to a delegate.

An example implementation for `ReplaceIcebergData` node:

```scala
override def getOutputs(context: OpenLineageContext): List[OutputDatasetWithFacets] = {
    if (!table.isInstanceOf[DataSourceV2Relation]) {
      List()
    } else {
      val relation = table.asInstanceOf[DataSourceV2Relation]
      val datasetFacetsBuilder: DatasetFacetsBuilder = {
        new OpenLineage.DatasetFacetsBuilder()
          .lifecycleStateChange(
          context
            .openLineage
            .newLifecycleStateChangeDatasetFacet(
              OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
              null
            )
        )
      }
      
      // enrich dataset with additional facets like a dataset version
      DatasetVersionUtils.getVersionOf(relation) match {
        case Some(version) => datasetFacetsBuilder.version(
          context
            .openLineage
            .newDatasetVersionDatasetFacet(version)
        )
        case None =>
      }

      // return output dataset while pointing that more dataset details shall be extracted from
      // `relation` object.
      List(
        OutputDatasetWithDelegate(
          relation,
          datasetFacetsBuilder,
          new OpenLineage.OutputDatasetOutputFacetsBuilder()
        )
      )
    }
  }
```

### When extension implements a relation within standard LogicalRelation

In this scenario, Spark extension is using standard `LogicalRelation` node within the logical plan.
However, the node may contain extension's specific `relation` property which extends
`org.apache.spark.sql.sources.BaseRelation`. In this case, we allow `BaseRelation` implementation
to implement `io.openlineage.spark.builtin.scala.v1.LineageRelation` interface.

### When extension implements a provider to create relations

An extension can contain implementation of `org.apache.spark.sql.sources.RelationProvider`
which again does not use any custom nodes within the logical plan, but provides classes to
create relations. To support this scenario, `io.openlineage.spark.builtin.scala.v1.LineageDatasetProvider`
can be implemented.

### When extension uses Spark DataSource v2 API

Some extensions rely on Spark DataSource V2 API and implement TableProvider, Table, ScanBuilder etc.
that are used within Spark to create `DataSourceV2Relation` instances.

A logical plan node `DataSourceV2Relation` contains `Table` field with a properties map of type
`Map<String, String>`. `openlineage-spark` uses this map to extract dataset information for lineage
event from `DataSourceV2Relation`. It is checking for the properties `openlineage.dataset.name` and
`openlineage.dataset.namespace`. If they are present, it uses them to identify a dataset. Please
be aware that namespace and name need to conform to [naming convention](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).

Properties can be also used to pass any dataset facet. For example:
```
openlineage.dataset.facets.customFacet={"property1": "value1", "property2": "value2"}
```
will enrich dataset with `customFacet`:
```json
"inputs": [{
"name": "...",
"namespace": "...",
"facets": {
    "customFacet": {
        "property1": "value1",
        "property2": "value2",
        "_producer": "..."
    },
    "schema": { }
}]
```