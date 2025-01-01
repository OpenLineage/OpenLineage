/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Interface for classes implementing {@code org.apache.spark.sql.sources.RelationProvider}.
 *
 * <p>The {@code RelationProvider} interface defines the {@code createRelation} method, which takes
 * {@code SQLContext} and {@code parameters} as arguments. This interface enables lineage extraction
 * from relation providers without directly depending on Spark's code, as the code may vary across
 * different Spark versions.
 *
 * <p>To align with the {@code createRelation} method, the {@code getLineageDatasetIdentifier}
 * method in this interface is designed to accept similar arguments. When implementing this method,
 * classes can provide two versions: one that matches the arguments of {@code createRelation}, and
 * another that throws an exception if it is ever called, ensuring compatibility across different
 * implementations.
 */
public interface LineageRelationProvider {

  /**
   * Returns a {@link DatasetIdentifier} containing the namespace and name of the dataset for
   * lineage tracking purposes.
   *
   * @param sparkListenerEventName the name of the Spark listener event triggering the lineage
   *     extraction
   * @param openLineage an instance of {@link OpenLineage} used for lineage-related operations
   * @param sqlContext the SQL context, typically used in Spark SQL queries
   * @param parameters the parameters used by the relation provider to create the relation
   * @return a {@link DatasetIdentifier} representing the dataset associated with the event
   */
  DatasetIdentifier getLineageDatasetIdentifier(
      String sparkListenerEventName, OpenLineage openLineage, Object sqlContext, Object parameters);
}
