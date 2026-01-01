/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.CachedData;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.internal.SharedState;
import scala.collection.IndexedSeq;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} for Spark 3.
 */
@Slf4j
public class PlanUtils3 {

  // Ensure that resources like this SparkSession object are closed after use -> we
  // don't want to close SparkSession
  @SuppressWarnings("PMD")
  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {

    if (!context.getSparkSession().isPresent()) {
      throw new IllegalArgumentException("SparkSession cannot be empty");
    }

    try {
      return (Optional.of(
          CatalogUtils3.getDatasetIdentifier(context, catalog, identifier, properties)));
    } catch (UnsupportedCatalogException ex) {
      log.warn(
          String.format(
              "Catalog %s is unsupported: %s",
              catalog.getClass().getCanonicalName(), ex.getMessage()));
      return Optional.empty();
    } catch (MissingDatasetIdentifierCatalogException ex) {
      log.debug(
          String.format(
              "Catalog %s is missing dataset identifier: %s",
              catalog.getClass().getCanonicalName(), ex.getMessage()));
      return Optional.empty();
    } catch (Exception e) {
      if (e instanceof NoSuchTableException) {
        // probably trying to obtain table details on START event while table does not exist
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * If a {@link SparkPlan} contains {@link InMemoryRelation}, this means it uses a cached dataset
   * that was evaluated within different {@link SparkPlan}. This method is used to return a {@link
   * LogicalPlan} of a job that evaluated a cached dataset. This is achieved through a reflection
   * from {@link CacheManager} available in a shared state of spark session.
   *
   * @param context
   * @param inMemoryRelation
   * @return
   */
  public static Optional<LogicalPlan> getLogicalPlanOf(
      OpenLineageContext context, InMemoryRelation inMemoryRelation) {
    try {
      SharedState sharedState = context.getSparkSession().get().sharedState();
      CacheManager cacheManager =
          (CacheManager)
              FieldUtils.getField(SharedState.class, "cacheManager", true).get(sharedState);
      IndexedSeq<CachedData> cachedDataIndexedSeq =
          (IndexedSeq<CachedData>)
              FieldUtils.getField(CacheManager.class, "cachedData", true).get(cacheManager);

      return ScalaConversionUtils.<CachedData>fromSeq(cachedDataIndexedSeq).stream()
          .filter(
              cachedData ->
                  cachedData
                      .cachedRepresentation()
                      .cacheBuilder()
                      .cachedName()
                      .equals(inMemoryRelation.cacheBuilder().cachedName()))
          .map(cachedData -> (LogicalPlan) cachedData.plan())
          .findAny();
    } catch (Exception e) {
      log.warn("cannot extract logical plan", e);
      // do nothing
    }
    return Optional.empty();
  }
}
