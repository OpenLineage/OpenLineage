/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.openlineage.spark.agent.TestIds;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A {@link StorageBackend} rooted at {@code <containerUri>/<runId>} on Azure ADLS Gen2 (abfss). The
 * run id keeps cleanup scoped to this run, never the container root. close() deletes the run root
 * via a Hadoop {@link FileSystem} built from the given {@link AbfsStorageConfig} credentials. The
 * configuration is self-contained (it does not depend on the SparkSession), so close() also works
 * from the shutdown hook, after the shared SparkContext may already be stopped.
 */
@Slf4j
public final class AbfsStorageBackend implements StorageBackend {
  private final String root;
  private final Configuration hadoopConf;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Thread shutdownHook = new Thread(this::close);

  public AbfsStorageBackend(String containerUri, AbfsStorageConfig storageConfig) {
    this.root = stripTrailingSlash(containerUri) + "/" + randomRunId();
    this.hadoopConf = buildHadoopConf(storageConfig);
    // Safety net so a crashed test (one whose tearDown never ran) does not leak its run root in the
    // container. A no-op after a normal close(), which deregisters this hook.
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  private static Configuration buildHadoopConf(AbfsStorageConfig storageConfig) {
    Configuration conf = new Configuration();
    storageConfig.toHadoopConfig().forEach(conf::set);
    return conf;
  }

  private static String stripTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  private static String randomRunId() {
    return "run_" + TestIds.randomHex();
  }

  @Override
  public String location(String relativePath) {
    if (closed.get()) {
      throw new IllegalStateException("AbfsStorageBackend has been closed");
    }
    return root + "/" + relativePath;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // Drop the safety-net hook now that we are cleaning up explicitly; ignore the failure that
      // occurs when close() is itself being called from the shutdown hook.
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException alreadyShuttingDown) {
        // JVM is already in shutdown — the hook is running, nothing to remove.
      }
      Path rootPath = new Path(root);
      try {
        FileSystem fs = rootPath.getFileSystem(hadoopConf);
        if (fs.exists(rootPath)) {
          fs.delete(rootPath, true);
        }
      } catch (Exception e) {
        log.warn("Failed to clean up abfss test root: {}", root, e);
      }
    }
  }
}
