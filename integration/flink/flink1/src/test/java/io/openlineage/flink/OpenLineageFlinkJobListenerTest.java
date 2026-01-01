/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.flink.utils.ClassUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class OpenLineageFlinkJobListenerTest {

  @Test
  void testJobListenerFailsForFlink2() {
    try (MockedStatic<ClassUtils> contextFactory = mockStatic(ClassUtils.class)) {
      when(ClassUtils.hasFlink2Classes()).thenReturn(true);
      assertThatThrownBy(
              () ->
                  OpenLineageFlinkJobListener.builder()
                      .executionEnvironment(mock(StreamExecutionEnvironment.class))
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("OpenLineageFlinkJobListener detected Flink 2 classes.");
    }
  }
}
