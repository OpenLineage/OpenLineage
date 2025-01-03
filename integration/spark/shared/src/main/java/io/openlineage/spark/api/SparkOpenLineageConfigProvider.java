/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

/**
 * Interface to provide the SparkOpenLineageConfig. Used when implementing custom configuration
 * loading.
 */
public interface SparkOpenLineageConfigProvider {
  SparkOpenLineageConfig getSparkOpenLineageConfig();
}
