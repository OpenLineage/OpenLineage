/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Seq$;

class SqlDwRelationParams {
  private String jdbcUrlField;
  private String somethingElse;

  private String jdbcUrl() {
    return jdbcUrlField;
  }

  private String getSomethingElse() {
    return somethingElse;
  }

  public SqlDwRelationParams(String jdbcUrl) {
    this.jdbcUrlField = jdbcUrl;
    somethingElse = "ABC";
  }
}

class MockSqlDWBaseRelation extends BaseRelation {
  private final String tableNameOrSubquery;
  private final Object params;

  @Override
  public SQLContext sqlContext() {
    return null;
  }

  @Override
  public StructType schema() {
    return new StructType(
        new StructField[] {new StructField("name", StringType$.MODULE$, false, null)});
  }

  public MockSqlDWBaseRelation(String tableNameOrSubquery, String jdbcUrl) {
    this.tableNameOrSubquery = tableNameOrSubquery;
    this.params = new SqlDwRelationParams(jdbcUrl);
  }
}

class MockSpark2SqlDWBaseRelation extends BaseRelation {
  private final String com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery;
  private final Object params;

  @Override
  public SQLContext sqlContext() {
    return null;
  }

  @Override
  public StructType schema() {
    return new StructType(
        new StructField[] {new StructField("name", StringType$.MODULE$, false, null)});
  }

  public MockSpark2SqlDWBaseRelation(String tableNameOrSubquery, String jdbcUrl) {
    this.com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery = tableNameOrSubquery;
    this.params = new SqlDwRelationParams(jdbcUrl);
  }
}

class TestSqlDWDatabricksVisitor extends SqlDWDatabricksVisitor {
  public TestSqlDWDatabricksVisitor(OpenLineageContext context, DatasetFactory factory) {
    super(context, factory);
  }

  @Override
  protected boolean isSqlDwRelationClass(LogicalPlan plan) {
    return true;
  }
}

class SQLDWDatabricksVisitorTest {
  SparkSession session = mock(SparkSession.class);
  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    when(context.getOpenLineage())
        .thenReturn(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI));
  }

  @Test
  void testSQLDWRelation() {
    String inputName = "\"dbo\".\"table1\"";
    String inputJdbcUrl =
        "jdbc:sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB";
    String expectedName = "dbo.table1";
    String expectedNamespace =
        "sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB;";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSqlDWBaseRelation(inputName, inputJdbcUrl),
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    TestSqlDWDatabricksVisitor visitor =
        new TestSqlDWDatabricksVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(1, datasets.size());
    OpenLineage.Dataset ds = datasets.get(0);
    assertEquals(expectedNamespace, ds.getNamespace());
    assertEquals(expectedName, ds.getName());
  }

  @Test
  void testSpark2SQLDWRelation() {
    String inputName = "\"dbo\".\"table1\"";
    String inputJdbcUrl =
        "jdbc:sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB";
    String expectedName = "dbo.table1";
    String expectedNamespace =
        "sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB;";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSpark2SqlDWBaseRelation(inputName, inputJdbcUrl),
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    TestSqlDWDatabricksVisitor visitor =
        new TestSqlDWDatabricksVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(1, datasets.size());
    OpenLineage.Dataset ds = datasets.get(0);
    assertEquals(expectedNamespace, ds.getNamespace());
    assertEquals(expectedName, ds.getName());
  }

  @Test
  void testSQLDWRelationComplexQuery() {
    String inputName = "(SELECT * FROM dbo.table1) q";
    String inputJdbcUrl =
        "jdbc:sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB";
    String expectedName = "COMPLEX";
    String expectedNamespace =
        "sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB;";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSqlDWBaseRelation(inputName, inputJdbcUrl),
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    TestSqlDWDatabricksVisitor visitor =
        new TestSqlDWDatabricksVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(1, datasets.size());
    OpenLineage.Dataset ds = datasets.get(0);
    assertEquals(expectedNamespace, ds.getNamespace());
    assertEquals(expectedName, ds.getName());
  }

  @Test
  void testSQLDWRelationBadJdbcUrl() {
    String inputName = "dbo.mytable";
    String inputJdbcUrl = "sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSqlDWBaseRelation(inputName, inputJdbcUrl),
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    TestSqlDWDatabricksVisitor visitor =
        new TestSqlDWDatabricksVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(0, datasets.size());
  }
}
