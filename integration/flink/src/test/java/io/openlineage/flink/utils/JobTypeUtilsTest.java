/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.junit.jupiter.api.Test;

public class JobTypeUtilsTest {

  @Test
  void testExtractWhenExecutionModeIsBatch() {
    assertThat(JobTypeUtils.extract(RuntimeExecutionMode.BATCH, Collections.emptyList()))
        .isEqualTo(JobTypeUtils.BATCH);
  }

  @Test
  void testExtractWhenExecutionModeIsStreaming() {
    assertThat(JobTypeUtils.extract(RuntimeExecutionMode.STREAMING, Collections.emptyList()))
        .isEqualTo(JobTypeUtils.STREAMING);
  }

  @Test
  void testExtractWhenExecutionModeIsAutomaticAndNoUnboundedDatasets() {
    Transformation dataset =
        mock(Transformation.class, withSettings().extraInterfaces(WithBoundedness.class));
    when(((WithBoundedness) dataset).getBoundedness()).thenReturn(Boundedness.CONTINUOUS_UNBOUNDED);
    List<Transformation<?>> transformations = Collections.singletonList(dataset);

    assertThat(JobTypeUtils.extract(RuntimeExecutionMode.AUTOMATIC, transformations))
        .isEqualTo(JobTypeUtils.STREAMING);
  }

  @Test
  void testExtractWhenExecutionModeIsAutomaticWithUnboundedDatasets() {
    Transformation dataset =
        mock(Transformation.class, withSettings().extraInterfaces(WithBoundedness.class));
    when(((WithBoundedness) dataset).getBoundedness()).thenReturn(Boundedness.BOUNDED);
    List<Transformation<?>> transformations = Collections.singletonList(dataset);

    assertThat(JobTypeUtils.extract(RuntimeExecutionMode.AUTOMATIC, transformations))
        .isEqualTo(JobTypeUtils.BATCH);
  }
}
