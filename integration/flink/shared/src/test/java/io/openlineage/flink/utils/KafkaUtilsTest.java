/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaUtilsTest {

  Properties properties = mock(Properties.class);

  @Test
  void testDatasetIdentifierOfSingleServer() {
    when(properties.getProperty("bootstrap.servers")).thenReturn("kafka-server1");

    assertThat(KafkaUtils.datasetIdentifierOf(properties, "topic"))
        .hasFieldOrPropertyWithValue("name", "topic")
        .hasFieldOrPropertyWithValue("namespace", "kafka://kafka-server1");
  }

  @Test
  void testDatasetIdentifierOfMultipleServersSeparatedBySemicolon() {
    when(properties.getProperty("bootstrap.servers")).thenReturn("kafka-server1;kafka-server2");

    assertThat(KafkaUtils.datasetIdentifierOf(properties, "topic"))
        .hasFieldOrPropertyWithValue("name", "topic")
        .hasFieldOrPropertyWithValue("namespace", "kafka://kafka-server1");
  }

  @Test
  void testDatasetIdentifierOfMultipleServersSeparatedByComma() {
    when(properties.getProperty("bootstrap.servers")).thenReturn("kafka-server1,kafka-server2");

    assertThat(KafkaUtils.datasetIdentifierOf(properties, "topic"))
        .hasFieldOrPropertyWithValue("name", "topic")
        .hasFieldOrPropertyWithValue("namespace", "kafka://kafka-server1");
  }
}
