/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

/**
 * Captures the lineage information produced while Spark materializes a checkpointed {@link
 * org.apache.spark.sql.Dataset}.
 *
 * <p>Spark runs {@code Dataset.checkpoint()}/{@code localCheckpoint()} as a separate {@code
 * SQLExecution} whose logical plan is no longer retrievable once the checkpoint completes - unlike
 * caching, there is no {@code CacheManager}-like registry to look it up later. We therefore capture
 * the input datasets and column lineage eagerly, while the plan is still available (i.e. while
 * handling the checkpoint's own {@code SparkListenerSQLExecutionEnd}), and reuse them later when a
 * {@link org.apache.spark.sql.execution.LogicalRDD} wrapping the checkpointed RDD is found in a
 * downstream query's plan.
 */
@Getter
public class Checkpoint {

  private final List<OpenLineage.InputDataset> inputDatasets;
  private final OpenLineage.ColumnLineageDatasetFacetFields columnLineageFields;

  /**
   * Dataset-level column lineage dependencies captured for the plan being checkpointed - i.e.
   * columns used for filtering, sorting, grouping, joining, window functions, etc., which are not
   * necessarily tied to any specific output field (and may not even appear in the checkpoint's
   * output schema at all). These are kept separate from {@link #columnLineageFields} because they
   * cannot be bridged by matching output attribute names downstream.
   */
  private final List<OpenLineage.InputField> datasetDependencyFields;

  public Checkpoint(
      List<OpenLineage.InputDataset> inputDatasets,
      OpenLineage.ColumnLineageDatasetFacetFields columnLineageFields,
      List<OpenLineage.InputField> datasetDependencyFields) {
    this.inputDatasets = inputDatasets == null ? Collections.emptyList() : inputDatasets;
    this.columnLineageFields = columnLineageFields;
    this.datasetDependencyFields =
        datasetDependencyFields == null ? Collections.emptyList() : datasetDependencyFields;
  }
}
