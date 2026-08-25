/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.spark.agent.lifecycle.plan.catalog.RelationHandler;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg.IcebergRelationHandler;
import java.util.List;

/** Relation handlers available to every Spark 3.x / 4.x version. */
final class DefaultRelationHandlers {
  private DefaultRelationHandlers() {}

  static List<RelationHandler> list(OpenLineageContext context) {
    return ImmutableList.of(new IcebergRelationHandler(context));
  }
}
