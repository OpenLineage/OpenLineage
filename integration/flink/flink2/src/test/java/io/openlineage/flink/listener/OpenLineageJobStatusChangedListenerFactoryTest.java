/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.junit.jupiter.api.Test;

class OpenLineageJobStatusChangedListenerFactoryTest {

  private final Context context = mock(Context.class, RETURNS_DEEP_STUBS);
  private final OpenLineageJobStatusChangedListenerFactory factory =
      new OpenLineageJobStatusChangedListenerFactory();

  @Test
  void testDefaultCreatesOpenLineageJobStatusChangedListener() {
    when(context.getConfiguration()).thenReturn(openLineageConfiguration(Map.of()));

    JobStatusChangedListener listener = factory.createListener(context);

    assertThat(listener).isInstanceOf(OpenLineageJobStatusChangedListener.class);
  }

  @Test
  void testDetachedModeCreatesOpenLineageDetachedJobStatusChangedListener() {
    when(context.getConfiguration())
        .thenReturn(
            openLineageConfiguration(
                Map.of("openlineage.flink.enableDetachedJobTracking", "true")));

    JobStatusChangedListener listener = factory.createListener(context);

    assertThat(listener).isInstanceOf(OpenLineageDetachedJobStatusChangedListener.class);
  }

  private Configuration openLineageConfiguration(Map<String, String> config) {
    Map<String, String> baseConfig = new HashMap<>();
    baseConfig.put("openlineage.transport.type", "file");
    baseConfig.put("openlineage.transport.location", "build/test_events/events");
    baseConfig.putAll(config);
    return Configuration.fromMap(baseConfig);
  }
}
