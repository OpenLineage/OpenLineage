/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkContainerProperties.SCALA_BINARY_VERSION;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_DOCKER_IMAGE;
import static io.openlineage.spark.agent.SparkContainerProperties.SPARK_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class SparkContainerPropertiesTest {
  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = "4\\..*")
  void spark4DockerImageUsesRequestedJavaVersion() {
    String dockerJavaVersion = System.getProperty("spark.docker.java.version");

    assertThat(SPARK_DOCKER_IMAGE)
        .isEqualTo(
            String.format(
                "apache/spark:%s-scala%s-java%s-python3-r-ubuntu",
                SPARK_VERSION, SCALA_BINARY_VERSION, dockerJavaVersion));
  }
}
