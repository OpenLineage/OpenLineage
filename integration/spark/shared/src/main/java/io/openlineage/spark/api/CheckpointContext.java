/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Keeps track of lineage captured for Spark checkpoints so it can be reattached later when a
 * checkpointed RDD is read downstream (see {@link Checkpoint}).
 *
 * <p>Local checkpoints are stored as persisted RDD blocks, so Spark posts {@code
 * SparkListenerUnpersistRDD} once they're unpersisted - explicitly or by {@code ContextCleaner}
 * once the RDD is garbage collected - after which they can't be read anymore and their entries are
 * removed (see {@link #removeRdd}). There is no such signal for non-local checkpoints, so the
 * lineage is additionally kept in a bounded, insertion-ordered map that evicts the oldest entry
 * once {@link SparkOpenLineageConfig.CheckpointConfig#getMaxEntries()} is exceeded. This keeps
 * memory usage bounded for long-running applications (e.g. structured streaming) at the cost of
 * losing lineage for checkpoints created long before they are read - which is no worse than the
 * lineage-less behavior prior to this feature.
 *
 * <p>Locally checkpointed RDDs are additionally claimed by the checkpoint execution that resolved
 * them (see {@link #claim}), so that executions of checkpoints created at the same call site (e.g.
 * in a loop) don't resolve the same RDD. Claims are kept apart from the evicting lineage map and
 * only released once the RDD is unpersisted, so neither a failed capture nor an eviction releases
 * an RDD that can still be resolved. Their number is bounded by the number of persisted local
 * checkpoints.
 *
 * <p>Entries are added and removed on the listener thread, but read while building input datasets,
 * which may run on {@code OpenLineageRunEventTimeoutExecutor}'s thread (and keep running after a
 * timeout), so access is synchronized.
 */
public class CheckpointContext {

  private final Map<Integer, Checkpoint> checkpointsByRddId;
  private final Set<Integer> claimedRddIds = new HashSet<>();

  public CheckpointContext() {
    this(SparkOpenLineageConfig.CheckpointConfig.DEFAULT_MAX_ENTRIES);
  }

  public CheckpointContext(int maxEntries) {
    checkpointsByRddId =
        new LinkedHashMap<Integer, Checkpoint>(16, 0.75f, false) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<Integer, Checkpoint> eldest) {
            return size() > maxEntries;
          }
        };
  }

  /**
   * Stores the lineage captured for the checkpoint materialized into the RDD with the given id.
   *
   * @param rddId id of the (checkpointed) {@link org.apache.spark.rdd.RDD} as reported by {@link
   *     org.apache.spark.sql.execution.LogicalRDD#rdd()}.
   */
  public synchronized void addCheckpoint(int rddId, Checkpoint checkpoint) {
    checkpointsByRddId.put(rddId, checkpoint);
  }

  /**
   * Marks the locally checkpointed RDD with the given id as resolved by a checkpoint execution, so
   * no other execution resolves it - see the class documentation.
   */
  public synchronized void claim(int rddId) {
    claimedRddIds.add(rddId);
  }

  /** Whether the RDD with the given id has already been claimed by a checkpoint execution. */
  public synchronized boolean isClaimed(int rddId) {
    return claimedRddIds.contains(rddId);
  }

  /**
   * Drops the lineage and the claim for the checkpoint materialized into the RDD with the given id,
   * if any. Called once the RDD is unpersisted, as a checkpoint can't be read anymore afterwards.
   */
  public synchronized void removeRdd(int rddId) {
    checkpointsByRddId.remove(rddId);
    claimedRddIds.remove(rddId);
  }

  /**
   * Returns the lineage captured for the checkpoint materialized into the RDD with the given id, if
   * it was captured and hasn't been evicted yet.
   *
   * @param rddId id of the (checkpointed) {@link org.apache.spark.rdd.RDD} as reported by {@link
   *     org.apache.spark.sql.execution.LogicalRDD#rdd()}.
   */
  public synchronized Optional<Checkpoint> getCheckpoint(int rddId) {
    return Optional.ofNullable(checkpointsByRddId.get(rddId));
  }
}
