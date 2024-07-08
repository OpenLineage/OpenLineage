/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.Option;

class SqlDwRelationParams {

  @SuppressWarnings("PMD") // used by reflection
  private String jdbcUrlField;

  @SuppressWarnings("PMD") // used by reflection
  private String somethingElse;

  @SuppressWarnings("PMD") // used by reflection
  private String jdbcUrl() {
    return jdbcUrlField;
  }

  @SuppressWarnings("PMD") // used by reflection
  private String getSomethingElse() {
    return somethingElse;
  }

  public SqlDwRelationParams(String jdbcUrl) {
    this.jdbcUrlField = jdbcUrl;
    somethingElse = "ABC";
  }
}

class MockSqlDWBaseRelation extends BaseRelation {
  @SuppressWarnings("PMD") // used by reflection
  private final String tableNameOrSubquery;

  @SuppressWarnings("PMD") // used by reflection
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
  private static final String FIELD_NAME = "name";

  @SuppressWarnings("PMD") // used by reflection
  private final String com$databricks$spark$sqldw$SqlDWRelation$$tableNameOrSubquery;

  @SuppressWarnings("PMD") // used by reflection
  private final Object params;

  @Override
  public SQLContext sqlContext() {
    return null;
  }

  @Override
  public StructType schema() {
    return new StructType(
        new StructField[] {new StructField(FIELD_NAME, StringType$.MODULE$, false, null)});
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
  private static final String FIELD_NAME = "name";
  SparkSession session = mock(SparkSession.class);
  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @ParameterizedTest
  @CsvSource({
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;database=MYTESTDB,sqlserver://mytestserver.database.windows.net,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;databaseName=MYTESTDB,sqlserver://mytestserver.database.windows.net,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net,sqlserver://mytestserver.database.windows.net,schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB,sqlserver://mytestserver.database.windows.net:1433,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;portNumber=1433;database=MYTESTDB,sqlserver://mytestserver.database.windows.net:1433,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net\\someinstance;database=MYTESTDB,sqlserver://mytestserver.database.windows.net/someinstance,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;instanceName=someinstance;database=MYTESTDB,sqlserver://mytestserver.database.windows.net/someinstance,MYTESTDB.schema.table1",
    "jdbc:sqlserver://;serverName=MYTESTSERVER.database.windows.net,sqlserver://mytestserver.database.windows.net,schema.table1",
  })
  void testSQLDWRelation(String inputJdbcUrl, String expectedNamespace, String expectedName) {
    String inputName = "\"schema\".\"table1\"";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSqlDWBaseRelation(inputName, inputJdbcUrl),
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
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

  @ParameterizedTest
  @CsvSource({
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;database=MYTESTDB,sqlserver://mytestserver.database.windows.net,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;databaseName=MYTESTDB,sqlserver://mytestserver.database.windows.net,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net,sqlserver://mytestserver.database.windows.net,schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net:1433;database=MYTESTDB,sqlserver://mytestserver.database.windows.net:1433,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;portNumber=1433;database=MYTESTDB,sqlserver://mytestserver.database.windows.net:1433,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net\\someinstance;database=MYTESTDB,sqlserver://mytestserver.database.windows.net/someinstance,MYTESTDB.schema.table1",
    "jdbc:sqlserver://MYTESTSERVER.database.windows.net;instanceName=someinstance;database=MYTESTDB,sqlserver://mytestserver.database.windows.net/someinstance,MYTESTDB.schema.table1",
    "jdbc:sqlserver://;serverName=MYTESTSERVER.database.windows.net,sqlserver://mytestserver.database.windows.net,schema.table1",
  })
  void testSpark2SQLDWRelation(String inputJdbcUrl, String expectedNamespace, String expectedName) {
    String inputName = "\"schema\".\"table1\"";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSpark2SqlDWBaseRelation(inputName, inputJdbcUrl),
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
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
    String expectedName = "MYTESTDB.COMPLEX";
    String expectedNamespace = "sqlserver://mytestserver.database.windows.net:1433";

    // Instantiate a MockSQLDWRelation
    LogicalRelation lr =
        new LogicalRelation(
            new MockSqlDWBaseRelation(inputName, inputJdbcUrl),
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
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
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
            Option.empty(),
            false);

    TestSqlDWDatabricksVisitor visitor =
        new TestSqlDWDatabricksVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(0, datasets.size());
  }
}
