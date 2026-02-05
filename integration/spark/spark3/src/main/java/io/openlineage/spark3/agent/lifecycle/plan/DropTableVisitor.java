/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class DropTableVisitor extends QueryPlanVisitor<DropTable, OpenLineage.OutputDataset> {

  private static final String RESOLVED_IDENTIFIER_CLASS_NAME =
      "org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier";

  public DropTableVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  /**
   * Check if ResolvedIdentifier class is available (Spark 3.4+). Note: Class loading is cached
   * internally by the JVM, so repeated calls are efficient.
   */
  private static boolean hasResolvedIdentifierClass() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(RESOLVED_IDENTIFIER_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static boolean isResolvedIdentifier(Object obj) {
    if (!hasResolvedIdentifierClass()) {
      return false;
    }
    try {
      Class<?> resolvedIdentifierClass =
          Thread.currentThread().getContextClassLoader().loadClass(RESOLVED_IDENTIFIER_CLASS_NAME);
      return resolvedIdentifierClass.isAssignableFrom(obj.getClass());
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    if (!(x instanceof DropTable)) {
      return false;
    }
    LogicalPlan child = ((DropTable) x).child();
    // Handle both ResolvedTable (Spark 3.2/3.3) and ResolvedIdentifier (Spark 3.4+/Databricks
    // 14.2+)
    return (child instanceof ResolvedTable) || isResolvedIdentifier(child);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    DropTable dropTable = (DropTable) x;
    LogicalPlan child = dropTable.child();

    if (child instanceof ResolvedTable) {
      return applyResolvedTable((ResolvedTable) child);
    } else if (isResolvedIdentifier(child)) {
      return applyResolvedIdentifier(child);
    }

    return Collections.emptyList();
  }

  private List<OpenLineage.OutputDataset> applyResolvedTable(ResolvedTable resolvedTable) {
    TableCatalog tableCatalog = resolvedTable.catalog();
    Map<String, String> tableProperties = resolvedTable.table().properties();
    Identifier identifier = resolvedTable.identifier();

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (di.isPresent()) {
      DatasetCompositeFacetsBuilder builder = outputDataset().createCompositeFacetBuilder();
      CatalogUtils3.getCatalogDatasetFacet(context, tableCatalog, tableProperties)
          .ifPresent(
              catalogDatasetFacet ->
                  builder.getFacets().catalog(catalogDatasetFacet.getCatalogDatasetFacet()));
      builder
          .getFacets()
          .schema(PlanUtils.schemaFacet(context.getOpenLineage(), resolvedTable.schema()));
      builder
          .getFacets()
          .lifecycleStateChange(
              context
                  .getOpenLineage()
                  .newLifecycleStateChangeDatasetFacet(
                      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP,
                      null));

      return Collections.singletonList(outputDataset().getDataset(di.get(), builder));
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Handle ResolvedIdentifier using reflection since it's only available in Spark 3.4+. For
   * ResolvedIdentifier, we can extract: - identifier() returns the Identifier - catalog() returns
   * the CatalogPlugin (which can be cast to TableCatalog) - schema() returns the StructType
   */
  private List<OpenLineage.OutputDataset> applyResolvedIdentifier(LogicalPlan resolvedIdentifier) {
    try {
      // Get catalog via reflection
      Method catalogMethod = resolvedIdentifier.getClass().getMethod("catalog");
      Object catalogObj = catalogMethod.invoke(resolvedIdentifier);

      if (!(catalogObj instanceof TableCatalog)) {
        log.debug(
            "ResolvedIdentifier catalog is not a TableCatalog: {}",
            catalogObj != null ? catalogObj.getClass().getName() : "null");
        return Collections.emptyList();
      }
      TableCatalog tableCatalog = (TableCatalog) catalogObj;

      // Get identifier via reflection
      Method identifierMethod = resolvedIdentifier.getClass().getMethod("identifier");
      Identifier identifier = (Identifier) identifierMethod.invoke(resolvedIdentifier);

      // Get schema via reflection
      Method schemaMethod = resolvedIdentifier.getClass().getMethod("schema");
      StructType schema = (StructType) schemaMethod.invoke(resolvedIdentifier);

      // ResolvedIdentifier doesn't have table properties, use empty map
      Map<String, String> tableProperties = new LinkedHashMap<>();

      Optional<DatasetIdentifier> di =
          PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

      if (di.isPresent()) {
        DatasetCompositeFacetsBuilder builder = outputDataset().createCompositeFacetBuilder();
        CatalogUtils3.getCatalogDatasetFacet(context, tableCatalog, tableProperties)
            .ifPresent(
                catalogDatasetFacet ->
                    builder.getFacets().catalog(catalogDatasetFacet.getCatalogDatasetFacet()));
        builder.getFacets().schema(PlanUtils.schemaFacet(context.getOpenLineage(), schema));
        builder
            .getFacets()
            .lifecycleStateChange(
                context
                    .getOpenLineage()
                    .newLifecycleStateChangeDatasetFacet(
                        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP,
                        null));

        return Collections.singletonList(outputDataset().getDataset(di.get(), builder));
      } else {
        return Collections.emptyList();
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.warn("Failed to extract dataset from ResolvedIdentifier", e);
      return Collections.emptyList();
    }
  }
}
