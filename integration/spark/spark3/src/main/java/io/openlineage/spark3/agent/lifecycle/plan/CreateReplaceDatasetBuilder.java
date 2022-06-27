/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.CreateV2Table;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateReplaceDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CreateReplaceDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return (x instanceof CreateTableAsSelect)
        || (x instanceof ReplaceTable)
        || (x instanceof ReplaceTableAsSelect)
        // Class CreateV2Table was removed in Spark Catalyst 3.3.0. For some reason, it is also
        // missing on Databricks platform when Spark context is in version 3.2.1. This hacky way
        // allows checking for the class also when it is not available on the class path
        || "org.apache.spark.sql.catalyst.plans.logical.CreateV2Table"
            .equals(x.getClass().getCanonicalName());
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    TableCatalog tableCatalog;
    Map<String, String> tableProperties;
    Identifier identifier;
    StructType schema;
    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycleStateChange;

    if (x instanceof CreateTableAsSelect) {
      CreateTableAsSelect command = (CreateTableAsSelect) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      lifecycleStateChange =
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
    } else if (x instanceof CreateV2Table) {
      CreateV2Table command = (CreateV2Table) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      lifecycleStateChange =
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
    } else if (x instanceof ReplaceTable) {
      ReplaceTable command = (ReplaceTable) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      lifecycleStateChange =
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
    } else {
      ReplaceTableAsSelect command = (ReplaceTableAsSelect) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      lifecycleStateChange =
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
    }

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (!di.isPresent()) {
      return Collections.emptyList();
    }

    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.DatasetFacetsBuilder builder =
        openLineage
            .newDatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(openLineage, schema))
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(lifecycleStateChange, null))
            .dataSource(PlanUtils.datasourceFacet(openLineage, di.get().getNamespace()));

    if (includeDatasetVersion(event)) {
      Optional<String> datasetVersion =
          CatalogUtils3.getDatasetVersion(tableCatalog, identifier, tableProperties);
      datasetVersion.ifPresent(
          version -> builder.version(openLineage.newDatasetVersionDatasetFacet(version)));
    }

    CatalogUtils3.getTableProviderFacet(tableCatalog, tableProperties)
        .map(provider -> builder.put("tableProvider", provider));
    return Collections.singletonList(
        outputDataset().getDataset(di.get().getName(), di.get().getNamespace(), builder.build()));
  }
}
