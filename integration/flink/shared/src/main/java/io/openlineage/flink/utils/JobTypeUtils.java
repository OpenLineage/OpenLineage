/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;

/**
 * Class to extract if a job is of streaming or batch type. It contains copy of code snippets from
 * Flink internals to determine a job type.
 */
public class JobTypeUtils {

  public static final String BATCH = "BATCH";
  public static final String STREAMING = "STREAMING";

  /**
   * Modified version of
   * https://github.com/apache/flink/blob/50ca1de29df7ca36c57e8524b5444a4d7529a5e1/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java#L494
   *
   * @param executionMode
   * @param transformations
   * @return
   */
  public static String extract(
      RuntimeExecutionMode executionMode, List<Transformation<?>> transformations) {
    if (executionMode != RuntimeExecutionMode.AUTOMATIC) {
      return (executionMode == RuntimeExecutionMode.BATCH) ? BATCH : STREAMING;
    }

    return existsUnboundedSource(transformations) ? STREAMING : BATCH;
  }

  private static boolean existsUnboundedSource(List<Transformation<?>> transformations) {
    return transformations.stream()
        .anyMatch(
            transformation ->
                isUnboundedSource(transformation)
                    || transformation.getTransitivePredecessors().stream()
                        .anyMatch(t -> isUnboundedSource(t)));
  }

  /**
   * https://github.com/apache/flink/blob/50ca1de29df7ca36c57e8524b5444a4d7529a5e1/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java#L524
   *
   * <p>Verifies if a transformation contains streaming source. Method copied from Flink internals,
   * as there is no way access it through reflection.
   *
   * @param transformation
   * @return
   */
  private static boolean isUnboundedSource(final Transformation<?> transformation) {
    return transformation instanceof WithBoundedness
        && ((WithBoundedness) transformation).getBoundedness() != Boundedness.BOUNDED;
  }
}
