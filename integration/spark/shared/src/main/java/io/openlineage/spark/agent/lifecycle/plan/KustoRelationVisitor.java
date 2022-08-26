/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches the KustoRelation that comes from the Azure Kusto (Azure
 * Data Factory). The KustoRelation is used to extract the table name and database name to populate
 * the {@link OpenLineage.Dataset} during Kusto read operations.
 */
@Slf4j
public class KustoRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> factory;
  private static final String KUSTO_CLASS_NAME =
      "com.microsoft.kusto.spark.datasource.KustoRelation";

  private static final String KUSTO_PROVIDER_CLASS_NAME =
      "com.microsoft.kusto.spark.datasource.DefaultSource";

  private static final String KUSTO_URL_SUFFIX = ".kusto.windows.net";

  private static final String KUSTO_PREFIX = "azurekusto://";

  public KustoRelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  protected boolean isKustoClass(LogicalPlan plan) {
    try {
      Class c = Thread.currentThread().getContextClassLoader().loadClass(KUSTO_CLASS_NAME);
      return (plan instanceof LogicalRelation
          && c.isAssignableFrom(((LogicalRelation) plan).relation().getClass()));
    } catch (Exception e) {
      // swallow - not a kusto class
    }
    return false;
  }

  public static boolean isKustoSource(CreatableRelationProvider provider) {
    try {
      Class c = Thread.currentThread().getContextClassLoader().loadClass(KUSTO_PROVIDER_CLASS_NAME);
      return c.isAssignableFrom(provider.getClass());
    } catch (Exception e) {
      // swallow - not a kusto source
    }
    return false;
  }

  public static boolean hasKustoClasses() {
    /**
     * Checking the Kusto class with both KustoRelationVisitor.class.getClassLoader.loadClass and
     * Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
     * present on the classpath, and the second one is a catchall which captures if the class has
     * been installed. This is relevant for Azure Databricks where jars can be installed and
     * accessible to the user, even if they are not present on the classpath.
     */
    try {
      KustoRelationVisitor.class.getClassLoader().loadClass(KUSTO_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(KUSTO_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }

    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isKustoClass(plan);
  }

  private static Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    try {
      Object query = FieldUtils.readField(relation, "query", true);
      tableName = (String) query;
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }

    if (StringUtils.isBlank(tableName)) {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }
    // Check if the query is complex
    if (StringUtils.countMatches(tableName, "|") > 0) {
      tableName = "COMPLEX";
    }

    return Optional.of(tableName);
  }

  private static Optional<String> getNamespace(BaseRelation relation) {
    String url;
    String databaseName;
    String kustoUrl;
    try {
      Object kustoCoords = FieldUtils.readField(relation, "kustoCoordinates", true);
      Object clusterUrl = FieldUtils.readField(kustoCoords, "clusterUrl", true);
      Object database = FieldUtils.readField(kustoCoords, "database", true);

      kustoUrl = (String) clusterUrl;
      kustoUrl = kustoUrl.replace("https://", "");
      databaseName = (String) database;
      url = String.format("%s%s/%s", KUSTO_PREFIX, kustoUrl, databaseName);
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover clusterUrl or database property");
      return Optional.empty();
    }
    if ("".equals(url)) {
      return Optional.empty();
    }

    return Optional.of(url);
  }

  public static <D extends OpenLineage.Dataset> List<D> createKustoDatasets(
      DatasetFactory<D> datasetFactory,
      scala.collection.immutable.Map<String, String> options,
      StructType schema) {
    // Called from SaveIntoDataSourceCommandVisitor on Kusto write operations.
    List<D> output;

    Map<String, String> javaOptions =
        io.openlineage.spark.agent.util.ScalaConversionUtils.fromMap(options);

    String name = javaOptions.get("kustotable");
    String database = javaOptions.get("kustodatabase");
    String kustoCluster = javaOptions.get("kustocluster");

    String namespace =
        String.format("%s%s%s/%s", KUSTO_PREFIX, kustoCluster, KUSTO_URL_SUFFIX, database);
    output = Collections.singletonList(datasetFactory.getDataset(name, namespace, schema));
    return output;
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    BaseRelation relation = ((LogicalRelation) x).relation();
    List<D> output;
    Optional<String> name = getName(relation);
    Optional<String> namespace = getNamespace(relation);
    if (name.isPresent() && namespace.isPresent()) {
      output =
          Collections.singletonList(
              factory.getDataset(name.get(), namespace.get(), relation.schema()));
    } else {
      output = Collections.emptyList();
    }
    return output;
  }
}
