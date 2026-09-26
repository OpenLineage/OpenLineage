/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/** A {@link StorageBackend} rooted at a local temp directory; close() recursively deletes it. */
@Slf4j
public final class LocalStorageBackend implements StorageBackend {
  private final Path root;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Thread shutdownHook = new Thread(this::close);

  public LocalStorageBackend() {
    try {
      root = Files.createTempDirectory("spark-external-tables");
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary directory for test storage", e);
    }
    // Safety net for a crashed test; deregistered by a normal close().
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  @Override
  public String location(String relativePath) {
    if (closed.get()) {
      throw new IllegalStateException("LocalStorageBackend has been closed");
    }
    return root.resolve(relativePath).toString();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException alreadyShuttingDown) {
        // JVM is already in shutdown — the hook is running, nothing to remove.
      }
      if (Files.exists(root)) {
        try {
          deleteRecursively(root);
        } catch (IOException e) {
          log.warn("Failed to clean up temporary directory: {}", root, e);
        }
      }
    }
  }

  private static void deleteRecursively(Path path) throws IOException {
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
