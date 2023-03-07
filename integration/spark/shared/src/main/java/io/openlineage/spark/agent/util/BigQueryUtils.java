/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.agent.lifecycle.plan.BigQueryNodeOutputVisitor;

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
}
