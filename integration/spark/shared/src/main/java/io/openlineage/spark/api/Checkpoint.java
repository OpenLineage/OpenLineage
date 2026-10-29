/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.CheckpointColumnLineage;
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
 *
 * <p>The lineage is resolved all the way down to the datasets read by the checkpointed plan, also
 * when they're read through other checkpoints. Each checkpoint is therefore self-contained, the
 * same way Spark truncates the RDD lineage at a checkpoint, and lineage of any other checkpoint can
 * be dropped without affecting it.
 */
@Getter
public class Checkpoint {

  private final List<OpenLineage.InputDataset> inputDatasets;
  private final CheckpointColumnLineage columnLineage;

  /**
   * Whether capturing the column lineage timed out (see {@code
   * spark.openlineage.timeout.buildDatasetsTimePercentage}), so only the input datasets were
   * captured. Runs reading the checkpoint report it in their {@code debug} facet.
   */
  private final boolean incomplete;

  public Checkpoint(
      List<OpenLineage.InputDataset> inputDatasets, CheckpointColumnLineage columnLineage) {
    this(inputDatasets, columnLineage, false);
  }

  public Checkpoint(
      List<OpenLineage.InputDataset> inputDatasets,
      CheckpointColumnLineage columnLineage,
      boolean incomplete) {
    this.inputDatasets = inputDatasets == null ? Collections.emptyList() : inputDatasets;
    this.columnLineage = columnLineage == null ? CheckpointColumnLineage.empty() : columnLineage;
    this.incomplete = incomplete;
  }
}
