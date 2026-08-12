/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogUtils;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import io.openlineage.spark3.agent.utils.CopyIntoSqlUtils;
import io.openlineage.spark3.agent.utils.UnityCatalogTableDatasetUtils;
import io.openlineage.spark3.agent.utils.UnityCatalogTableDatasetUtils.ResolvedTableDataset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;

/**
 * Extracts output datasets from the Databricks-specific {@code CopyIntoCommand} or {@code
 * CopyIntoCommandEdge}. Since these classes belong to the Databricks runtime and are not available
 * at compile time, reflection is used to access their target relation, with SQL parsing as a
 * fallback.
 */
@Slf4j
public class CopyIntoCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  private static final int CATALOG_SCHEMA_TABLE_PARTS = 3;
  private static final int SCHEMA_TABLE_PARTS = 2;

  public CopyIntoCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return CopyIntoCommandUtils.isCopyIntoCommand(x) || isOptimizedRootWithCopyIntoSql(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    if (CopyIntoCommandUtils.isCopyIntoCommand(x)) {
      List<OutputDataset> datasets = datasetsFromCommand(event, x);
      if (!datasets.isEmpty()) {
        return datasets;
      }
      log.warn(
          "Matched COPY INTO command {} but extracted no output datasets",
          x.getClass().getCanonicalName());
    } else if (isOptimizedRootWithCopyIntoSql(x)) {
      for (LogicalPlan command : CopyIntoCommandUtils.findAllCommands(context)) {
        List<OutputDataset> datasets = datasetsFromCommand(event, command);
        if (!datasets.isEmpty()) {
          return datasets;
        }
      }
    } else {
      return Collections.emptyList();
    }
    return datasetsFromSql(event);
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    Optional<String> suffix =
        CopyIntoCommandUtils.target(plan)
            .flatMap(
                target ->
                    context.getOutputDatasetBuilders().stream()
                        .filter(b -> b instanceof AbstractQueryPlanOutputDatasetBuilder)
                        .map(b -> (AbstractQueryPlanOutputDatasetBuilder) b)
                        .map(b -> b.jobNameSuffixFromLogicalPlan(target))
                        .filter(Optional::isPresent)
                        .map(o -> (String) o.get())
                        .findFirst());
    if (suffix.isPresent()) {
      return suffix;
    }
    if (isOptimizedRootWithCopyIntoSql(plan)) {
      return CopyIntoCommandUtils.sqlText(context)
          .flatMap(CopyIntoSqlUtils::targetTable)
          .map(table -> table.replace(".", "_"));
    }
    return Optional.empty();
  }

  private List<OutputDataset> datasetsFromCommand(SparkListenerEvent event, LogicalPlan command) {
    List<OutputDataset> fromTarget =
        CopyIntoCommandUtils.target(command)
            .map(target -> delegate(target, event))
            .orElse(Collections.emptyList());
    if (!fromTarget.isEmpty()) {
      return fromTarget;
    }
    List<OutputDataset> fromRelation =
        CopyIntoCommandUtils.findTableRelation(command)
            .map(relation -> delegate(relation, event))
            .orElse(Collections.emptyList());
    if (fromRelation.isEmpty()) {
      log.warn(
          "COPY INTO: no output dataset from command members or plan tree. {}",
          CopyIntoCommandUtils.describeMembers(command));
    }
    return fromRelation;
  }

  private List<OutputDataset> datasetsFromSql(SparkListenerEvent event) {
    return CopyIntoCommandUtils.sqlText(context)
        .flatMap(CopyIntoSqlUtils::targetTable)
        .map(tableName -> outputDatasetsFromTableName(tableName, event))
        .orElse(Collections.emptyList());
  }

  /**
   * Resolves the COPY INTO target from parsed SQL text. Does not call {@code session.table()}: on
   * Databricks that re-analyzes the table in the SparkListener thread and fails with {@code
   * MissingCredentialScopeException}.
   */
  private List<OutputDataset> outputDatasetsFromTableName(
      String tableName, SparkListenerEvent event) {
    return sparkSession()
        .map(
            session -> {
              TableIdentifier identifier =
                  parseTableIdentifier(tableName.replace("`", "").trim(), session);
              try {
                return outputDatasetFromTableName(tableName, event, session, identifier)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
              } catch (Exception e) {
                log.warn(
                    "COPY INTO: catalog resolution failed for target {}, falling back to name-only identity",
                    identifier,
                    e);
                return outputDatasetFromTableNameFallback(tableName, session, identifier)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
              }
            })
        .orElse(Collections.emptyList());
  }

  private boolean isOptimizedRootWithCopyIntoSql(LogicalPlan x) {
    return context.getQueryExecution().map(qe -> qe.optimizedPlan() == x).orElse(false)
        && CopyIntoCommandUtils.sqlText(context)
            .filter(CopyIntoSqlUtils::isCopyIntoStatement)
            .isPresent();
  }

  private Optional<OutputDataset> outputDatasetFromTableName(
      String tableName,
      SparkListenerEvent event,
      SparkSession session,
      TableIdentifier identifier) {
    Optional<OutputDataset> fromV2Catalog =
        UnityCatalogTableDatasetUtils.resolve(context, session, identifier)
            .map(resolved -> buildOutputDataset(resolved, event));
    if (fromV2Catalog.isPresent()) {
      return fromV2Catalog;
    }

    try {
      CatalogTable catalogTable = session.sessionState().catalog().getTableMetadata(identifier);
      return Optional.of(outputDataset().sparkDatasetBuilder().dataset(catalogTable).build());
    } catch (Exception e) {
      log.debug(
          "Legacy catalog lookup failed for COPY INTO target {}, trying name-only fallback",
          tableName,
          e);
    }

    return outputDatasetFromTableNameFallback(tableName, session, identifier);
  }

  private Optional<OutputDataset> outputDatasetFromTableNameFallback(
      String tableName, SparkSession session, TableIdentifier identifier) {
    if (DatabricksUtils.isDatabricksUnityCatalogEnabled(session.sparkContext().getConf())) {
      return Optional.of(
          outputDataset()
              .sparkDatasetBuilder()
              .dataset(
                  DatabricksUtils.qualifiedUnityCatalogTableName(identifier),
                  DatabricksUtils.UNITY_CATALOG_SYMLINK_NAMESPACE)
              .build());
    }
    log.warn("Unable to resolve COPY INTO target table {}", tableName);
    return Optional.empty();
  }

  private Optional<SparkSession> sparkSession() {
    Optional<SparkSession> session = context.getSparkSession();
    if (!session.isPresent()) {
      session = SparkSessionUtils.activeSession();
    }
    return session;
  }

  private OutputDataset buildOutputDataset(
      ResolvedTableDataset resolved, SparkListenerEvent event) {
    SparkDatasetBuilder<OutputDataset> sparkBuilder =
        outputDataset()
            .sparkDatasetBuilder()
            .dataset(resolved.getDatasetIdentifier())
            .schema(resolved.getSchema());
    try {
      if (includeDatasetVersion(event)) {
        CatalogUtils.getDatasetVersion(
                context, resolved.getCatalog(), resolved.getIdentifier(), resolved.getProperties())
            .ifPresent(sparkBuilder::version);
      }
      CatalogUtils.addStorageAndCatalogFacets(
          context, resolved.getCatalog(), resolved.getProperties(), sparkBuilder.getInner());
    } catch (Exception | NoSuchMethodError | NoClassDefFoundError e) {
      log.warn(
          "Could not add catalog facets for COPY INTO target {}",
          resolved.getDatasetIdentifier().getName(),
          e);
    }
    return sparkBuilder.build();
  }

  private TableIdentifier parseTableIdentifier(String normalized, SparkSession session) {
    String[] parts = normalized.split("\\.");
    String defaultDatabase = session.catalog().currentDatabase();
    Optional<String> defaultCatalog = currentCatalog(session);

    if (parts.length == CATALOG_SCHEMA_TABLE_PARTS) {
      return tableIdentifier(parts[2], parts[1], Optional.of(parts[0]));
    }
    if (parts.length == SCHEMA_TABLE_PARTS) {
      return tableIdentifier(parts[1], parts[0], defaultCatalog);
    }
    return tableIdentifier(parts[0], defaultDatabase, defaultCatalog);
  }

  private TableIdentifier tableIdentifier(String table, String database, Optional<String> catalog) {
    if (catalog.filter(StringUtils::isNotBlank).isPresent()) {
      try {
        return (TableIdentifier)
            MethodUtils.invokeMethod(
                TableIdentifier$.MODULE$,
                "apply",
                table,
                Option.apply(database),
                Option.apply(catalog.get()));
      } catch (Exception e) {
        log.debug(
            "Could not build TableIdentifier with catalog {}, falling back to schema only",
            catalog.get(),
            e);
      }
    }
    return TableIdentifier$.MODULE$.apply(table, Option.apply(database));
  }

  private Optional<String> currentCatalog(SparkSession session) {
    try {
      return Optional.ofNullable(
              (String) MethodUtils.invokeMethod(session.catalog(), "currentCatalog"))
          .filter(StringUtils::isNotBlank);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
