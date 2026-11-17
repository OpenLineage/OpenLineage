/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

/**
 * Lets a vendor report a different output for a file write.
 *
 * <p>Some connectors write files only as an intermediate step. For example, a connector may write
 * Parquet files to a temporary folder and then load them into a database table. Lineage should show
 * the table, not the temporary folder. A vendor returns resolvers from {@link
 * OpenLineageEventHandlerFactory#createFileWriteTargetResolvers}.
 */
@FunctionalInterface
public interface FileWriteTargetResolver {

  /**
   * @param command the file write
   * @return empty to report the write as usual, with its path as the output; otherwise the output
   *     to report instead
   */
  Optional<Target> resolve(InsertIntoHadoopFsRelationCommand command);

  /**
   * The output to report for a file write instead of its path.
   *
   * <p>The write's save mode applies to the intermediate files, not to this dataset, so the output
   * has no {@code lifecycleStateChange} facet.
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  class Target {
    /** The dataset to report, or empty to report no output at all. */
    Optional<DatasetIdentifier> dataset;

    public static Target of(DatasetIdentifier dataset) {
      return new Target(Optional.of(dataset));
    }

    /** Reports no output, for example when the connector's target can't be determined. */
    public static Target none() {
      return new Target(Optional.empty());
    }
  }
}
