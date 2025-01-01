/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;

/**
 * MeterRegistryFactory is an interface that defines methods to build different types of meter
 * registries from OpenLineage config.
 */
public interface MeterRegistryFactory<T extends MeterRegistry> {
  T registry(Map<String, Object> config);

  String type();
}
