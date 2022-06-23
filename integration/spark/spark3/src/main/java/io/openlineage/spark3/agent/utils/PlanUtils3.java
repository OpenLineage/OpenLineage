/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.IcebergHandler;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} for Spark 3.
 */
@Slf4j
public class PlanUtils3 {

  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context, DataSourceV2Relation relation) {
    return Optional.of(relation)
        .filter(r -> r.identifier() != null)
        .filter(r -> r.identifier().isDefined())
        .filter(r -> r.catalog() != null)
        .filter(r -> r.catalog().isDefined())
        .filter(r -> r.catalog().get() instanceof TableCatalog)
        .flatMap(
            r ->
                PlanUtils3.getDatasetIdentifier(
                    context,
                    (TableCatalog) r.catalog().get(),
                    r.identifier().get(),
                    r.table().properties()));
  }

  // Ensure that resources like this SparkSession object are closed after use -> we
  // don't want to close SparkSession
  @SuppressWarnings("PMD")
  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {

    SparkSession sparkSession =
        context
            .getSparkSession()
            .orElseThrow(() -> new IllegalArgumentException("SparkSession cannot be empty"));

    try {
      return (Optional.of(
          CatalogUtils3.getDatasetIdentifier(sparkSession, catalog, identifier, properties)));
    } catch (UnsupportedCatalogException ex) {
      log.error(String.format("Catalog %s is unsupported", ex.getMessage()), ex);
      return Optional.empty();
    }
  }

  public static void includeProviderFacet(
      TableCatalog catalog,
      Map<String, String> properties,
      Map<String, OpenLineage.DatasetFacet> facets) {
    Optional<TableProviderFacet> providerFacet =
        CatalogUtils3.getTableProviderFacet(catalog, properties);
    if (providerFacet.isPresent()) {
      facets.put("tableProvider", providerFacet.get());
    }
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory, OpenLineageContext context, DataSourceV2Relation relation) {
    return fromDataSourceV2Relation(
        datasetFactory, context, relation, context.getOpenLineage().newDatasetFacetsBuilder());
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory,
      OpenLineageContext context,
      DataSourceV2Relation relation,
      OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder) {

    if (relation.identifier().isEmpty()) {
      throw new IllegalArgumentException(
          "Couldn't find identifier for dataset in plan " + relation);
    }
    Identifier identifier = relation.identifier().get();

    if (relation.catalog().isEmpty() || !(relation.catalog().get() instanceof TableCatalog)) {
      throw new IllegalArgumentException("Couldn't find catalog for dataset in plan " + relation);
    }
    TableCatalog tableCatalog = (TableCatalog) relation.catalog().get();

    Map<String, String> tableProperties = relation.table().properties();
    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    OpenLineage openLineage = context.getOpenLineage();
    datasetFacetsBuilder
        .schema(PlanUtils.schemaFacet(openLineage, relation.schema()))
        .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

    CatalogUtils3.getTableProviderFacet(tableCatalog, tableProperties)
        .map(provider -> datasetFacetsBuilder.put("tableProvider", provider));
    return Collections.singletonList(
        datasetFactory.getDataset(
            di.get().getName(), di.get().getNamespace(), datasetFacetsBuilder.build()));
  }

  public static Optional<DataSourceV2Relation> getDataSourceV2Relation(LogicalPlan x) {
    NamedRelation table = null;

    // INSERT OVERWRITE TABLE SQL statement is translated into InsertIntoTable logical operator.
    if (x instanceof V2WriteCommand) {
      table = ((V2WriteCommand) x).table();
    } else if (x instanceof InsertIntoStatement) {
      table = (NamedRelation) ((InsertIntoStatement) x).table();
    } else if (new IcebergHandler().hasClasses() && x instanceof ReplaceData) {
      // DELETE FROM on ICEBERG HAS START ELEMENT WITH ReplaceData AND COMPLETE ONE WITH
      // DeleteFromTable
      table = ((ReplaceData) x).table();
    } else if (x instanceof DeleteFromTable) {
      table = (NamedRelation) ((DeleteFromTable) x).table();
    } else if (x instanceof UpdateTable) {
      table = (NamedRelation) ((UpdateTable) x).table();
    } else if (x instanceof MergeIntoTable) {
      table = (NamedRelation) ((MergeIntoTable) x).targetTable();
    }
    return Optional.ofNullable((DataSourceV2Relation) table);
  }
}
