/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps track of lineage captured for Spark checkpoints so it can be reattached later when a
 * checkpointed RDD is read downstream (see {@link Checkpoint}).
 *
 * <p>There is no reliable signal from Spark telling us when a checkpoint is no longer referenced by
 * any live RDD (unlike caching, which has a {@code ContextCleaner}/{@code onUnpersistRDD} hook), so
 * entries are kept in bounded, insertion-ordered maps that evict the oldest entry once {@link
 * #MAX_ENTRIES} is exceeded. This keeps memory usage bounded for long-running applications (e.g.
 * structured streaming) at the cost of losing lineage for checkpoints created long before they are
 * read - which is no worse than the lineage-less behavior prior to this feature.
 */
public class CheckpointContext {

  private static final int MAX_ENTRIES = 1000;

  private final Map<Long, Checkpoint> checkpoints = boundedMap();
  private final Map<Integer, Long> rddToExecutionIdMapping = boundedMap();

  private static <K, V> Map<K, V> boundedMap() {
    return new LinkedHashMap<K, V>(16, 0.75f, false) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > MAX_ENTRIES;
      }
    };
  }

  public void addCheckpoint(long executionId, Checkpoint checkpoint) {
    checkpoints.put(executionId, checkpoint);
  }

  public void addRddToExecutionIdMapping(int rddId, long executionId) {
    rddToExecutionIdMapping.put(rddId, executionId);
  }

  /**
   * Returns the lineage captured for the checkpoint materialized into the RDD with the given id, if
   * it was captured and hasn't been evicted yet.
   *
   * @param rddId id of the (checkpointed) {@link org.apache.spark.rdd.RDD} as reported by {@link
   *     org.apache.spark.sql.execution.LogicalRDD#rdd()}.
   */
  public Optional<Checkpoint> getCheckpoint(int rddId) {
    return Optional.ofNullable(rddToExecutionIdMapping.get(rddId)).map(checkpoints::get);
  }
}
