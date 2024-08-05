/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.OpenLineageSqlParserProvider;
import io.openlineage.spark.agent.lifecycle.RawSqlEventEmitter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

public class OpenLineageParserExtensionsProvider implements OpenLineageSqlParserProvider {

  public OpenLineageParserExtensionsProvider() {}

  @Override
  public ParserInterface provide(
      ParserInterface defaultParser, SparkSession spark, RawSqlEventEmitter eventEmitter) {
    return new OpenLineageParserExtensions(spark, defaultParser, eventEmitter);
  }
}
