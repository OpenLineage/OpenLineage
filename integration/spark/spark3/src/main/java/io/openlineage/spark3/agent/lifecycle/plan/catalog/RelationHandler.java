/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

// import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

public interface RelationHandler {
  boolean hasClasses();

  boolean isClass(DataSourceV2Relation relation);

  DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation);

  // default Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
  //   return Optional.empty();
  // }

  String getName();
}
