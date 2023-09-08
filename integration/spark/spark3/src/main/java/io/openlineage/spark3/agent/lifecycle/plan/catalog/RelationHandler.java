/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public interface RelationHandler {
  boolean hasClasses();

  boolean isClass(DataSourceV2Relation relation);

  DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation);

  String getName();
}
