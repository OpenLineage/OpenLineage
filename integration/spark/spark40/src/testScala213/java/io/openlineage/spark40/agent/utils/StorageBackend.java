/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

/**
 * Where integration tests put the data they create (e.g. Delta tables), abstracted over the
 * environment: a local temp dir, an {@code abfss://} container, etc.
 *
 * <p>Each instance owns a unique root. {@link #location} only hands out paths under it — the test
 * (via Spark) writes the data there; the backend never creates anything. {@link #close} is the
 * other half: it deletes the whole root, so the backend cleans up exactly what it handed out and
 * nothing else.
 */
public interface StorageBackend extends AutoCloseable {
  /** Composes the absolute location for {@code relativePath} under the root. Creates nothing. */
  String location(String relativePath);

  /** Deletes the root and everything under it. Idempotent and best-effort (logs, never throws). */
  @Override
  void close();
}
