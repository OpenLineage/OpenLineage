/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

public interface OpenLineageSqlParserProvider {
  ParserInterface provide(
      ParserInterface defaultParser, SparkSession spark, RawSqlEventEmitter eventEmitter);
}
