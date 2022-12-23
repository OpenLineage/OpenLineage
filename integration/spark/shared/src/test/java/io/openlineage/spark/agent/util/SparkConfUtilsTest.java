/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

public class SparkConfUtilsTest {

  SparkConf conf = new SparkConf().set("existing.property", "test");

  @Test
  void testNonExistingConfigEntry() {
    Optional<String> propertyValue =
        SparkConfUtils.findSparkConfigKey(conf, "non.existing.property");
    assertEquals(Optional.empty(), propertyValue);
  }

  @Test
  void testExistingConfigEntry() {
    Optional<String> propertyValue = SparkConfUtils.findSparkConfigKey(conf, "existing.property");
    assertEquals(Optional.of("test"), propertyValue);
  }

  @Test
  void testExistingConfigEntryWithTransportPrefix() {
    Optional<String> propertyValue =
        SparkConfUtils.findSparkConfigKey(conf, "transport.existing.property");
    assertEquals(Optional.of("test"), propertyValue);
  }
}
