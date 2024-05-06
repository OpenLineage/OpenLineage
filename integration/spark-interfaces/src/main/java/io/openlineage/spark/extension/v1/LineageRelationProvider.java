/**
 * Copyright 2018-2024 contributors to the OpenLineage project SPDX-License-Identifier: Apache-2.0
 */
package io.openlineage.spark.extension.v1;

import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Interface for classes implementing `org.apache.spark.sql.sources.RelationProvider`.
 * `RelationProvider` implements `createRelation` with `SQLContext` and `parameters` as arguments.
 * We want this package to not depend on Spark's code which may be different across Spark versions.
 *
 * <p>We're aiming to have arguments of `getLineageDataset` the same as arguments of
 * `createRelation` within `RelationProvider`. When implementing this method, one can provide two
 * implementations: one with arguments exactly the same as with `RelationProvider`, and other
 * throwing an exception which should never be called.
 */
public interface LineageRelationProvider {

  DatasetIdentifier getLineageDatasetIdentifier(
      OpenLineageExtensionContext openLineageExtensionContext,
      Object sqlContext,
      Object parameters);
}
