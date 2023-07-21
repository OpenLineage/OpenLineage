/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeOutputVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class BigQueryUtils {

  public static boolean hasBigQueryClasses() {
    try {
      BigQueryNodeOutputVisitor.class
          .getClassLoader()
          .loadClass("com.google.cloud.spark.bigquery.BigQueryRelation");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  private static Optional<Object> extractDatasetIdentifierFromTableId(Object tableId) {
    return Stream.of(
            ReflectionUtils.tryExecuteStaticMethodForClassName(
                "com.google.cloud.bigquery.connector.common.BigQueryUtil",
                "friendlyTableName",
                tableId),
            ReflectionUtils.tryExecuteStaticMethodForClassName(
                "com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryUtil",
                "friendlyTableName",
                tableId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

  public static List<DatasetIdentifier> extractDatasetIdentifier(
      BigQueryRelation bigQueryRelation) {

    return tryExecuteMethod(bigQueryRelation, "getTableId")
        .flatMap(BigQueryUtils::extractDatasetIdentifierFromTableId)
        .map(x -> new DatasetIdentifier((String) x, "namespace"))
        .map(Collections::singletonList)
        .orElseGet(Collections::emptyList);
  }
}
