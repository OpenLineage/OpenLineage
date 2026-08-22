/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import scala.collection.immutable.Map;

public class FakeDeltaProvider implements CreatableRelationProvider {

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
    return null;
  }
}
