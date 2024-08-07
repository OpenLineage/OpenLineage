/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.spark.agent.lifecycle.RawSqlEventEmitter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

public class OpenLineageParserExtensions implements ParserInterface {

  ParserInterface defaultParser;
  SparkSession spark;

  RawSqlEventEmitter eventEmitter;

  public OpenLineageParserExtensions(
      SparkSession spark, ParserInterface defaultParser, RawSqlEventEmitter eventEmitter) {
    this.defaultParser = defaultParser;
    this.spark = spark;
    this.eventEmitter = eventEmitter;
  }

  @Override
  public LogicalPlan parsePlan(String sqlText) throws ParseException {
    eventEmitter.sendSqlEvent(sqlText);
    return defaultParser.parsePlan(sqlText);
  }

  @Override
  public Expression parseExpression(String sqlText) throws ParseException {
    return defaultParser.parseExpression(sqlText);
  }

  @Override
  public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
    return defaultParser.parseTableIdentifier(sqlText);
  }

  @Override
  public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
    return defaultParser.parseFunctionIdentifier(sqlText);
  }

  @Override
  public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
    return defaultParser.parseMultipartIdentifier(sqlText).toList().toSeq();
  }

  @Override
  public StructType parseTableSchema(String sqlText) throws ParseException {
    return defaultParser.parseTableSchema(sqlText);
  }

  @Override
  public DataType parseDataType(String sqlText) throws ParseException {
    return defaultParser.parseDataType(sqlText);
  }
}
