package io.openlineage.spark.extension.scala.v1

import io.openlineage.client.utils.DatasetIdentifier

/** Trait to be implemented for extension's classes extending
  * `org.apache.spark.sql.sources.BaseRelation`. Implementing it allows
  * extracting lineage from such objects. Implementing `getNamespace` and
  * `getName` within the `DatasetIdentifier` is obligatory.
  */
trait LineageRelation {
  def getLineageDatasetIdentifier(
      openLineageContext: OpenLineageExtensionContext
  ): DatasetIdentifier
}

/** Trait for classes implementing
  * `org.apache.spark.sql.sources.RelationProvider`. `RelationProvider`
  * implements `createRelation` with `SQLContext` and `parameters` as arguments.
  * We want this package to not depend on Spark's code which may be different
  * across Spark versions.
  *
  * We're aiming to have arguments of `getLineageDataset` the same as arguments
  * of `createRelation` within `RelationProvider`. When implementing this
  * method, one can provide two implementations: one with arguments exactly the
  * same as with `RelationProvider`, and other throwing an exception which
  * should never be called.
  */
trait LineageRelationProvider {
  def getLineageDatasetIdentifier(
      openLineageContext: OpenLineageExtensionContext,
      sqlContext: Object,
      parameters: Object
  ): DatasetIdentifier
}
