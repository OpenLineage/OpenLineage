/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.nio.file.Path;
import java.nio.file.Paths;

final class SparkContainerProperties {
  private SparkContainerProperties() {}

  public static final String SPARK_VERSION = System.getProperty("spark.version");
  public static final String SCALA_BINARY_VERSION = System.getProperty("scala.binary.version");
  public static final String SPARK_DOCKER_IMAGE = System.getProperty("spark.docker.image");
  public static final String SCALA_JAR_NAME = System.getProperty("scala.fixtures.jar.name");
  public static final Path HOST_LIB_DIR = Paths.get(System.getProperty("lib.dir")).toAbsolutePath();
  public static final Path HOST_DEPENDENCIES_DIR =
      Paths.get(System.getProperty("dependencies.dir")).toAbsolutePath();
  public static final Path HOST_RESOURCES_DIR =
      Paths.get(System.getProperty("resources.dir")).toAbsolutePath();
  public static final Path HOST_LOG4J_PROPERTIES_PATH =
      HOST_RESOURCES_DIR.resolve("log4j.properties").toAbsolutePath();
  public static final Path HOST_LOG4J2_PROPERTIES_PATH =
      HOST_RESOURCES_DIR.resolve("log4j2.properties").toAbsolutePath();
  //  public static final Path HOST_FIXTURES_DIR =
  //      Paths.get(System.getProperty("fixtures.dir")).toAbsolutePath();
  public static final Path HOST_SCALA_FIXTURES_JAR_PATH =
      Paths.get(System.getProperty("scala.fixtures.jar.path")).toAbsolutePath();
  public static final Path HOST_ADDITIONAL_JARS_DIR =
      Paths.get(System.getProperty("additional.jars.dir")).toAbsolutePath();
  public static final Path HOST_ADDITIONAL_CONF_DIR =
      Paths.get(System.getProperty("additional.conf.dir")).toAbsolutePath();

  public static final Path CONTAINER_SPARK_HOME_DIR =
      Paths.get(System.getProperty("spark.home.dir"));
  public static final Path CONTAINER_SPARK_JARS_DIR = CONTAINER_SPARK_HOME_DIR.resolve("jars");
  public static final Path CONTAINER_SPARK_CONF_DIR = CONTAINER_SPARK_HOME_DIR.resolve("conf");
  public static final Path CONTAINER_FIXTURES_DIR =
      CONTAINER_SPARK_HOME_DIR.resolve("work/fixtures");
  public static final Path CONTAINER_FIXTURES_JAR_PATH =
      CONTAINER_FIXTURES_DIR.resolve(SCALA_JAR_NAME);
  public static final Path CONTAINER_LOG4J_PROPERTIES_PATH =
      CONTAINER_SPARK_CONF_DIR.resolve("log4j.properties");
  public static final Path CONTAINER_LOG4J2_PROPERTIES_PATH =
      CONTAINER_SPARK_CONF_DIR.resolve("log4j2.properties");
}
