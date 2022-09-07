/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.spark.agent.util.SparkConfUtils;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

public class SparkConfUtilsTest {

  SparkConf conf = new SparkConf();

  @Test
  void testFindSparkConfigKeyDouble() {
    conf.set("openlineage.timeout", "30.0");
    assertEquals(
        new Double(30), SparkConfUtils.findSparkConfigKeyDouble(conf, "openlineage.timeout").get());
  }

  @Test
  void testFindSparkConfigKeyDoubleWhenEntryWithSparkPrefix() {
    conf.set("spark.openlineage.timeout", "30.0");
    assertEquals(
        new Double(30), SparkConfUtils.findSparkConfigKeyDouble(conf, "openlineage.timeout").get());
  }

  @Test
  void testFindSparkConfigKeyDoubleWhenNoEntry() {
    assertEquals(
        Optional.empty(), SparkConfUtils.findSparkConfigKeyDouble(conf, "openlineage.timeout"));
  }
}
