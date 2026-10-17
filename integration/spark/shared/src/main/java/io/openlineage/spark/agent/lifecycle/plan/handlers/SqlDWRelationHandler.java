/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.handlers;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.DatasetFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;

/**
 * Extracts an {@link OpenLineage.Dataset} from a {@link LogicalRelation} over the SqlDWRelation
 * that comes from an Azure Databricks environment. This function extracts a {@link
 * OpenLineage.Dataset} from the SQL DW/ Synapse table referenced by the relation. The convention
 * used for a namespace is a URI of <code>
 * sqlserver://&lt;server&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The name for Sql Dw
 * tables may be table name (e.g. "exampleInputA") or a multi-part name (e.g.
 * "[dbo].[exampleInputA]"). If the data source is a query (e.g. <code>
 * ((select \"id\" FROM dbo.exampleInputA WHERE postalCode != '55555') q)</code>) then the name will
 * be <code>COMPLEX</code>.
 */
@Slf4j
public class SqlDWRelationHandler<D extends OpenLineage.Dataset> {
  private final DatasetFactory<D> factory;
  private static final String DATABRICKS_CLASS_NAME = "com.databricks.spark.sqldw.SqlDWRelation";
  private static final String TABLE_FIELD_NAME = "tableNameOrSubquery";

  public SqlDWRelationHandler(DatasetFactory<D> factory) {
    this.factory = factory;
  }

  public static boolean hasSqlDWDatabricksClasses() {
    try {
      SqlDWRelationHandler.class.getClassLoader().loadClass(DATABRICKS_CLASS_NAME);
      return true;
    } catch (NoClassDefFoundError | Exception e) {
      // If class does not exist or it's loading fails for some reason, we handle that failure by
      // returning false
    }
    return false;
  }

  public static boolean isSqlDWRelation(BaseRelation relation) {
    return relation != null
        && DATABRICKS_CLASS_NAME.equals(relation.getClass().getName())
        && hasSqlDWDatabricksClasses();
  }

  private Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    // The Databricks SqlDwRelation has a tableNameOrSubQuery field that
    // is defined as com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery
    // in their Spark 2 implementation. Therefore, we have to check for either
    // of those fields to extract the
    try {
      List<Field> relationFields = FieldUtils.getAllFieldsList(relation.getClass());
      for (Field f : relationFields) {
        if (TABLE_FIELD_NAME.equals(f.getName())) {
          tableName = (String) FieldUtils.readField(relation, TABLE_FIELD_NAME, true);
        }
      }
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover SQLDW tableNameOrSubquery property");
      return Optional.empty();
    }
    if ("".equals(tableName)) {
      log.warn("Unable to discover SQLDW tableNameOrSubquery property");
      return Optional.empty();
    }

    // The Synapse connector will return a table name wrapped in double quotes
    // or you could have a query string (e.g. (SELECT * FROM table)q)
    if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
      tableName = tableName.replace("\"", "");
    }
    // TODO If there is a query, we should ultimately parse the SQL but
    // returning COMPLEX to be consistent with other implementations.
    if (tableName.startsWith("(")) {
      tableName = "COMPLEX";
    }

    return Optional.of(tableName);
  }

  private Optional<String> getJdbcUrl(BaseRelation relation) {
    try {
      Object fieldDetails = FieldUtils.readField(relation, "params", true);
      String jdbcUrl = (String) MethodUtils.invokeMethod(fieldDetails, true, "jdbcUrl");
      if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
        return Optional.of(jdbcUrl);
      }
    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      log.warn("Unable to discover SQLDW jdbcUrl Parameters");
    }
    return Optional.empty();
  }

  public List<D> handleRelation(LogicalRelation logicalRelation) {
    BaseRelation relation = logicalRelation.relation();
    Optional<String> name = getName(relation);
    Optional<String> jdbcUrl = getJdbcUrl(relation);
    if (!name.isPresent() || !jdbcUrl.isPresent()) {
      return Collections.emptyList();
    }
    DatasetIdentifier di =
        JdbcDatasetUtils.getDatasetIdentifier(jdbcUrl.get(), name.get(), new Properties());
    return Collections.singletonList(
        factory.sparkDatasetBuilder().dataset(di).schema(relation.schema()).build());
  }
}
