/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.column;

import static io.openlineage.client.utils.SnowflakeUtils.parseAccountIdentifier;
import static io.openlineage.client.utils.SnowflakeUtils.stripQuotes;
import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;
import static io.openlineage.spark.agent.vendor.snowflake.Constants.SNOWFLAKE_PREFIX;
import static io.openlineage.spark.agent.vendor.snowflake.SnowflakeTable.getQualifiedName;
import static java.util.Arrays.stream;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.SqlCollector;
import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.Parameters.MergedParameters;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.sql.catalyst.expressions.Attribute;

@Slf4j
class SnowflakeColumnLineageVisitorDelegate {
  private final SqlMeta sqlMeta;
  private final String namespace;
  private final String database;
  private final String schema;
  private final ColumnLevelLineageContext context;
  private final SqlCollector sqlCollector;

  public SnowflakeColumnLineageVisitorDelegate(
      ColumnLevelLineageContext context,
      SnowflakeRelation snowflakeRelation,
      List<Attribute> attributes) {
    this.context = context;

    MergedParameters params = snowflakeRelation.params();
    this.database = stripQuotes(params.sfDatabase());
    this.schema = stripQuotes(params.sfSchema());
    this.namespace = SNOWFLAKE_PREFIX + parseAccountIdentifier(params.sfFullURL());

    this.sqlMeta = extractQueryFromSnowflake(snowflakeRelation).orElse(null);
    this.sqlCollector =
        new SqlCollector(
            this.context, this.sqlMeta, extractDatasetIdentifiers(this.sqlMeta), attributes);
  }

  public boolean isDefinedAt() {
    return sqlMeta != null;
  }

  public void collectInputs() {
    sqlCollector.collectInputs(
        dbTableMeta ->
            new DatasetIdentifier(getQualifiedName(database, schema, dbTableMeta), namespace));
  }

  public void collectExpressionDependencies() {
    sqlCollector.collectExpressionDependencies();
  }

  private List<DatasetIdentifier> extractDatasetIdentifiers(SqlMeta sqlMeta) {
    if (sqlMeta == null) {
      return Collections.emptyList();
    }

    return sqlMeta.inTables().stream()
        .map(
            table ->
                context
                    .getNamespaceResolver()
                    .resolve(
                        new DatasetIdentifier(
                            getQualifiedName(database, schema, table), namespace)))
        .collect(Collectors.toList());
  }

  private Optional<SqlMeta> extractQueryFromSnowflake(SnowflakeRelation snowflakeRelation) {
    MergedParameters params = snowflakeRelation.params();
    // https://github.com/snowflakedb/spark-snowflake/blob/3a26f61d51e6e50b4ea75d441527ed76bd3b534a/src/main/scala/net/snowflake/spark/snowflake/Parameters.scala#L541
    // Snowflake Spark Connector allows a subquery to be specified in `dbtable` parameter in
    // parentheses,
    // if that is the case, the value here will be None
    // if there are no parentheses, it is safe to assume the value is a simple table name.
    Optional<String> dbtable = asJavaOptional(params.table()).map(TableName::toString);
    if (dbtable.isPresent()) {
      DbTableMeta origin = new DbTableMeta(database, schema, stripQuotes(dbtable.get()));
      return Optional.of(
          new SqlMeta(
              Collections.singletonList(origin),
              Collections.emptyList(),
              stream(snowflakeRelation.schema().fields())
                  .map(
                      field ->
                          new ColumnLineage(
                              new ColumnMeta(null, field.name()),
                              Collections.singletonList(new ColumnMeta(origin, field.name()))))
                  .collect(Collectors.toList()),
              Collections.emptyList()));
    }

    // https://github.com/snowflakedb/spark-snowflake/blob/3a26f61d51e6e50b4ea75d441527ed76bd3b534a/src/main/scala/net/snowflake/spark/snowflake/Parameters.scala#L557
    // Value of `query` parameter if present, otherwise either:
    // - the subquery value from `dbtable` stripped of parentheses.
    // - None if `dbtable` is a simple table name.
    Optional<String> query = asJavaOptional(params.query()).map(Object::toString);
    if (!query.isPresent()) {
      return Optional.empty();
    }

    return OpenLineageSql.parse(Collections.singletonList(query.get()), "snowflake");
  }
}
