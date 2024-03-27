/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.metrics;

import org.junit.jupiter.api.BeforeEach;

public class BaseMetricsTest {
  @BeforeEach
  void clear() {
    MicrometerProvider.clear();
  }
}
