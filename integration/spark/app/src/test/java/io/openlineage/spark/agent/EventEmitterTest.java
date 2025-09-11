/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.api.SparkOpenLineageConfig;
import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

class EventEmitterTest {
  @SneakyThrows
  @Test
  void testApplicationNameOverride() {
    String appName = "test-app";
    String overrideName = "override-app";

    SparkConf sparkConf =
        new SparkConf().setAppName(appName).set(ArgumentParser.SPARK_CONF_APP_NAME, overrideName);
    SparkOpenLineageConfig olConfig = ArgumentParser.parse(sparkConf);
    EventEmitter eventEmitter = new EventEmitter(olConfig, appName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(overrideName);
  }
}
