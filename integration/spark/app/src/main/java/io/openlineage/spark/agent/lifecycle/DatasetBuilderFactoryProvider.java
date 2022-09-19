/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.package$;

public class DatasetBuilderFactoryProvider {

  private static final String SPARK2_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark2DatasetBuilderFactory";
  private static final String SPARK3_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark3DatasetBuilderFactory";
  private static final String SPARK32_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark32DatasetBuilderFactory";
  private static final String SPARK33_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark33DatasetBuilderFactory";

  static DatasetBuilderFactory getInstance() {
    String version = package$.MODULE$.SPARK_VERSION();
    try {
      return (DatasetBuilderFactory)
          Class.forName(getDatasetBuilderFactoryForVersion(version)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Can't instantiate dataset builder factory factory for version: %s", version),
          e);
    }
  }

  static String getDatasetBuilderFactoryForVersion(String version) {
    if (version.startsWith("2.")) {
      return SPARK2_FACTORY_NAME;
    } else if (version.startsWith("3.2")) {
      return SPARK32_FACTORY_NAME;
    } else if (version.startsWith("3.3")) {
      return SPARK33_FACTORY_NAME;
    } else {
      return SPARK3_FACTORY_NAME;
    }
  }
}
