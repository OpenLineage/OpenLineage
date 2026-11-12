/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
  void usesSparkJobNamespaceWhenNotConfigured() {
    SparkConf conf = new SparkConf(false).setAppName("orders");
    EventEmitter emitter = new EventEmitter(ArgumentParser.parse(conf), "orders", conf);
    assertThat(emitter.getJobNamespace()).isEqualTo("spark-jobs");
  }

  @SneakyThrows
  @Test
  void keepsExplicitDefaultJobNamespace() {
    SparkConf conf =
        new SparkConf(false).setAppName("orders").set("spark.openlineage.namespace", "default");
    EventEmitter emitter = new EventEmitter(ArgumentParser.parse(conf), "orders", conf);
    assertThat(emitter.getJobNamespace()).isEqualTo("default");
  }

  @Test
  void namespaceMergeDistinguishesUnsetFromExplicitDefault() {
    SparkOpenLineageConfig configured = new SparkOpenLineageConfig();
    configured.setNamespace("custom");
    SparkOpenLineageConfig unset = new SparkOpenLineageConfig();
    assertThat(unset.getNamespace()).isEqualTo("spark-jobs");
    assertThat(configured.mergeWithNonNull(unset).getNamespace()).isEqualTo("custom");
    for (String namespace : new String[] {"default", "spark-jobs"}) {
      SparkOpenLineageConfig override = new SparkOpenLineageConfig();
      override.setNamespace(namespace);
      assertThat(configured.mergeWithNonNull(override).getNamespace()).isEqualTo(namespace);
    }
  }

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

  @SneakyThrows
  @Test
  void testApplicationJobNameCanBeUpdatedAfterConstruction() {
    String appName = "raw-spark-app-name";

    SparkConf sparkConf = new SparkConf().setAppName(appName);
    SparkOpenLineageConfig olConfig = ArgumentParser.parse(sparkConf);
    EventEmitter eventEmitter = new EventEmitter(olConfig, appName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(appName);

    String resolvedName = "glue_job_name";
    eventEmitter.setApplicationJobName(resolvedName);

    assertThat(eventEmitter.getApplicationJobName()).isEqualTo(resolvedName);
  }

  @SneakyThrows
  @Test
  void keepsConfiguredJobNamespaceWithoutSharedRegistry() {
    SparkConf conf =
        new SparkConf(false)
            .setAppName("native-tables")
            .set("spark.openlineage.namespace", "spark-jobs");

    EventEmitter emitter = new EventEmitter(ArgumentParser.parse(conf), "native-tables", conf);

    assertThat(emitter.getJobNamespace()).isEqualTo("spark-jobs");
  }

  @SneakyThrows
  @Test
  void keepsConfiguredJobNamespaceWithSharedRegistry() {
    SparkConf conf =
        new SparkConf(false)
            .setAppName("hive-tables")
            .set("spark.openlineage.namespace", "spark-jobs")
            .set("spark.openlineage.dataset.identity.sources", "{\"sources\":[]}");

    EventEmitter emitter = new EventEmitter(ArgumentParser.parse(conf), "hive-tables", conf);

    assertThat(emitter.getJobNamespace()).isEqualTo("spark-jobs");
  }
}
