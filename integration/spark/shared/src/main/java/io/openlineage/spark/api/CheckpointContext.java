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
 * <p>Local checkpoints are stored as persisted RDD blocks, so Spark posts {@code
 * SparkListenerUnpersistRDD} once they're unpersisted - explicitly or by {@code ContextCleaner}
 * once the RDD is garbage collected - after which they can't be read anymore and their entries are
 * removed (see {@link #removeRdd}). There is no such signal for non-local checkpoints, so all
 * entries are additionally kept in bounded, insertion-ordered maps that evict the oldest entry once
 * {@link #MAX_ENTRIES} is exceeded. This keeps memory usage bounded for long-running applications
 * (e.g. structured streaming) at the cost of losing lineage for checkpoints created long before
 * they are read - which is no worse than the lineage-less behavior prior to this feature.
 *
 * <p>Entries are added and removed on the listener thread, but read while building input datasets,
 * which may run on {@code OpenLineageRunEventTimeoutExecutor}'s thread (and keep running after a
 * timeout), so access is synchronized.
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

  public synchronized void addCheckpoint(long executionId, Checkpoint checkpoint) {
    checkpoints.put(executionId, checkpoint);
  }

  public synchronized void addRddToExecutionIdMapping(int rddId, long executionId) {
    rddToExecutionIdMapping.put(rddId, executionId);
  }

  /**
   * Drops the lineage captured for the checkpoint materialized into the RDD with the given id, if
   * any. Called once the RDD is unpersisted, as a checkpoint can't be read anymore afterwards.
   */
  public synchronized void removeRdd(int rddId) {
    Long executionId = rddToExecutionIdMapping.remove(rddId);
    if (executionId != null) {
      checkpoints.remove(executionId);
    }
  }

  /** Whether the RDD with the given id has already been mapped to a checkpoint execution. */
  public synchronized boolean isRddMapped(int rddId) {
    return rddToExecutionIdMapping.containsKey(rddId);
  }

  /**
   * Returns the lineage captured for the checkpoint materialized into the RDD with the given id, if
   * it was captured and hasn't been evicted yet.
   *
   * @param rddId id of the (checkpointed) {@link org.apache.spark.rdd.RDD} as reported by {@link
   *     org.apache.spark.sql.execution.LogicalRDD#rdd()}.
   */
  public synchronized Optional<Checkpoint> getCheckpoint(int rddId) {
    return Optional.ofNullable(rddToExecutionIdMapping.get(rddId)).map(checkpoints::get);
  }
}
