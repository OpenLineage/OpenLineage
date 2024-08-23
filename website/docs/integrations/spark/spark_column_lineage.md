---
sidebar_position: 7
title: Column-Level Lineage
---

:::warning

Column-level lineage works only with Spark 3.
:::

:::info
Column-level lineage for Spark is turned on by default and requires no additional work to be done. The following documentation describes its internals. 
:::

:::info
Lineage contains information about what fields were used to create of influence the field but also how, see [Transformation Types](spec/facets/dataset-facets/column_lineage_facet.md#transformation-type)
:::

Column-level lineage provides fine-grained information on datasets dependencies. Not only do we know the dependency exists, but we are also able to understand which input columns are used to produce output columns. This allows for answering questions like *Which root input columns are used to construct column x?* 

## Standard specification

Collected information is sent in OpenLineage event within `columnLineage` dataset facet described [here](spec/facets/dataset-facets/column_lineage_facet.md). 

## Code architecture and its mechanics

Column-level lineage has been implemented separately from the rest of builders and visitors extracting lineage information from Spark logical plans. As a result the codebase is stored in `io.openlineage.spark3.agent.lifecycle.plan.columnLineage` package within classes responsible only for this feature.

* Class `ColumnLevelLineageUtils.java` is an entry point to run the mechanism and is used within `OpenLineageRunEventBuilder`.

* Classes `ColumnLevelLineageUtilsNonV2CatalogTest` and `ColumnLevelLineageUtilsV2CatalogTest` contain real-life test cases which run Spark jobs and get an access to the last query plan executed.
  They evaluate column-level lineage based on the plan and expected output schema.
  Then, they verify if this meets the requirements.
  This allows testing column-level lineage behavior in real scenarios. The more tests and scenarios put here, the better.

* Class `ColumnLevelLineageBuilder` contains both the logic of building output facet (`ColumnLineageDatasetFacetFields`) 
and datastructures containing necessary information:
  * schema - `SchemaDatasetFacet` contains information about output schema 
  * inputs - map pointing from `ExprId` to column name and `DatasetIdentifier` identifying the datasource 
  * outputs - map pointing from output field name to its `ExprId`
  * exprDependencies - map pointing from `ExprId` to set of its `Dependency` objects containing `ExprId` and information about type of the dependency.
  * datasetDependencies - list of `ExprId` representing pseudo-expressions representing operations like `filter`, `join` etc.
  * externalExpressionMappings - map pointing from `ColumnMeta` object to `ExprId` used for dependencies extracted by `sql-parser`


* Class `ColumnLevelLineageBuilder` is used when traversing logical plans to store all the information required to produce column-level lineage.
  It allows storing input/output columns. It also stores dependencies between the expressions contained in query plan.
  Once inputs, outputs and dependencies are filled, build method is used to produce output facet (`ColumnLineageDatasetFacetFields`).

* `OutputFieldsCollector` class is used to traverse the plan to gather the `outputs`, 
even though the information about output dataset is already in `schema`, it's not coupled information about the outputs `ExprId`.
The collector traverses the plan and matches the outputs existing there, inside `Aggregate` or `Project` objects, with the ones in `schema` by their name.

* `InputFieldsCollector` class is used to collect the inputs which can be extracted from `DataSourceV2Relation`, `DataSourceV2ScanRelation`, `HiveTableRelation` or `LogicalRelation`. 
Each input field has its `ExprId` within the plan. Each input is identified by `DatasetIdentifier`, which means it contains name and namespace, of a dataset and an input field.

* `ExpressionDependenciesCollector` traverses the plan to identify dependencies between different expressions using their `ExprId`. Dependencies map parent expressions to its dependencies with additional information about the transformation type. 
This is used evaluate which inputs influenced certain output and what kind of influence was it.

### Expression dependency collection process
  
For each node in `LogicalPlan` the `ExpressionDependencyCollector` attempts to extract the column lineage information based on its type.
First it goes through `ColumnLineageVisitors` to check if any applies to current node, if so then it extract dependencies from them.
Next if the node is `LogicalRelation` and relation type is `JDBCRelation`, the sql-parser extracts lineage data from query string itself.

:::warning

Because Sql parser only parses the query string in `JDBCRelation` it does not collect information about input field types or transformation types.
The only info collected is the name of the table/view and field, as it is mentioned in the query.
:::

After that all that's left are following types of nodes: `Project`,`Aggregate`, `Join`, `Filter`, `Sort`. 
Each of them contains dependency expressions that can be added to one of the lists `expressions` or `datasetDependencies`.

When node is `Aggregate`, `Join`, `Filter` or `Sort` it contains dependencies that don't affect one single output but all the outputs, 
so they need to be treated differently than normal dependencies.
For each of those nodes the new `ExprId` is created to represent "all outputs", all its dependencies will be of `INDIRECT` type.

For each of the `expressions` the collector tries to go through it and possible children expressions and add them to `exprDependencies` map with appropriate transformation type and `masking` flag.
Most of the expressions represent `DIRECT` transformation, only exceptions are `If` and `CaseWhen` which contain condition expressions.

### Facet building process

For each of the outputs `ColumnLevelLineageBuilder` goes through the `exprDependencies` to build the list final dependencies, then using `inputs` maps them to fields in datasets.
During the process it also unravels the transformation type between the input and output. 
To unravel two dependencies implement following logic:
- if current type is `INDIRECT` the result takes the type and subtype from current
- if current type is `DIRECT` and other one is null, result is null
- if current type is `DIRECT` and other is `INDIRECT` the result takes type and subtype from other
- if both are `DIRECT` the result is type `DIRECT`, subtype is the first existing from the order `AGGREGATION`, `TRANSFORMATION`, `IDENTITY`
- if any of the transformations is masking, the result is masking

The inputs are also mapped for all dataset dependencies. The result is added to each output. 
Finally, the list of outputs with all their inputs is mapped to `ColumnLineageDatasetFacetFields` object.

## Writing custom extensions

Spark framework is known for its great ability to be extended by custom libraries capable of reading or writing to anything. In case of having a custom implementation, we prepared an ability to extend column-level lineage implementation to be able to retrieve information from other input or output LogicalPlan nodes. 

Creating such an extension requires implementing a following interface: 

```
/** Interface for implementing custom collectors of column-level lineage. */
interface CustomColumnLineageVisitor {

  /**
   * Collect inputs for a given {@link LogicalPlan}. Column-level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Input information
   * should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectInputs(LogicalPlan node, ColumnLevelLineageBuilder builder);

  /**
   * Collect outputs for a given {@link LogicalPlan}. Column-level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Output information
   * should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectOutputs(LogicalPlan node, ColumnLevelLineageBuilder builder);

  /**
   * Collect expressions for a given {@link LogicalPlan}. Column-level lineage mechanism traverses
   * LogicalPlan on its node. This method will be called for each traversed node. Expression
   * dependency information should be put into builder.
   *
   * @param node
   * @param builder
   */
  void collectExpressionDependencies(LogicalPlan node, ColumnLevelLineageBuilder builder);
}
```
and making it available for Service Loader (implementation class name has to be put in a resource file `META-INF/services/io.openlineage.spark.agent.lifecycle.plan.column.CustomColumnLineageVisitor`).
