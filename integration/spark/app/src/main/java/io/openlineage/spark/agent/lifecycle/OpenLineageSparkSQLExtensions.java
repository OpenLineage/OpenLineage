/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.package$;
import org.apache.spark.sql.SparkSessionExtensions;
import scala.Function1;
import scala.runtime.BoxedUnit;

public class OpenLineageSparkSQLExtensions implements Function1<SparkSessionExtensions, BoxedUnit> {

  static String SPARK_2_PARSER_PROVIDER =
      "io.openlineage.spark2.agent.lifecycle.plan.OpenLineageParserExtensionsProvider";
  static String SPARK_32_PARSER_PROVIDER =
      "io.openlineage.spark32.agent.lifecycle.plan.OpenLineageParserExtensionsProvider";
  static String SPARK_3_PARSER_PROVIDER =
      "io.openlineage.spark33.agent.lifecycle.plan.OpenLineageParserExtensionsProvider";
  static String SPARK_40_PARSER_PROVIDER =
      "io.openlineage.spark40.agent.lifecycle.plan.OpenLineageParserExtensionsProvider";

  static OpenLineageSqlParserProvider resolveParserProvider() {
    String version = package$.MODULE$.SPARK_VERSION();
    String parserClassName = resolveParserClass(version);
    try {
      return (OpenLineageSqlParserProvider) Class.forName(parserClassName).newInstance();
    } catch (Exception e) {

      throw new RuntimeException(
          String.format("Failed to instantiate parser class: %s", version), e);
    }
  }

  static String resolveParserClass(String version) {
    if (version.startsWith("2.")) {
      return SPARK_2_PARSER_PROVIDER;
    } else if (version.startsWith("3.2")) {
      return SPARK_32_PARSER_PROVIDER;
    } else if (version.startsWith("3.3")) {
      return SPARK_3_PARSER_PROVIDER;
    } else if (version.startsWith("3.4")) {
      return SPARK_3_PARSER_PROVIDER;
    } else if (version.startsWith("3.5")) {
      return SPARK_3_PARSER_PROVIDER;
    } else if (version.startsWith("4")) {
      return SPARK_40_PARSER_PROVIDER;
    } else {
      return SPARK_3_PARSER_PROVIDER;
    }
  }

  @Override
  public BoxedUnit apply(SparkSessionExtensions v1) {
    OpenLineageSqlParserProvider parserProvider = resolveParserProvider();
    v1.injectParser(
        (spark, defaultParser) ->
            parserProvider.provide(defaultParser, spark, new OpenLineageRawSqlEventEmitter()));
    return BoxedUnit.UNIT;
  }
}
