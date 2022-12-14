/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Field;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Schema;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableInfo;
import com.google.cloud.spark.bigquery.repackaged.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.CatalogFileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.text.TextFileFormat;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map$;

class LogicalPlanSerializerTest {
  private static final String TEST_DATA = "test_data";
  private static final String NAME = "name";
  private static final String TEST = "test";
  private static final String RESOURCES = "resources";
  private static final String SRC = "src";
  private static final String SERDE = "serde";
  private static final String EXPR_ID = "exprId";
  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final LogicalPlanSerializer logicalPlanSerializer = new LogicalPlanSerializer();

  @Test
  void testSerializeLogicalPlan() throws IOException {
    String jdbcUrl = "jdbc:postgresql://postgreshost:5432/sparkdata";
    String sparkTableName = "my_spark_table";
    scala.collection.immutable.Map<String, String> map =
        (scala.collection.immutable.Map<String, String>)
            Map$.MODULE$
                .<String, String>newBuilder()
                .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                .result();
    JDBCRelation relation =
        new JDBCRelation(
            new StructType(
                new StructField[] {
                  new StructField(NAME, StringType$.MODULE$, false, Metadata.empty())
                }),
            new Partition[] {},
            new JDBCOptions(jdbcUrl, sparkTableName, map),
            mock(SparkSession.class));
    LogicalRelation logicalRelation =
        new LogicalRelation(
            relation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);
    Aggregate aggregate =
        new Aggregate(
            Seq$.MODULE$.<Expression>empty(),
            Seq$.MODULE$.<NamedExpression>empty(),
            logicalRelation);

    Map<String, Object> aggregateActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(aggregate), mapTypeReference);
    Map<String, Object> logicalRelationActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedAggregateNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "aggregate-node.json");
    Path logicalRelationNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "logicalrelation-node.json");

    Map<String, Object> expectedAggregateNode =
        objectMapper.readValue(expectedAggregateNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedLogicalRelationNode =
        objectMapper.readValue(logicalRelationNodePath.toFile(), mapTypeReference);

    assertThat(aggregateActualNode).satisfies(new MatchesMapRecursively(expectedAggregateNode));
    assertThat(logicalRelationActualNode)
        .satisfies(new MatchesMapRecursively(expectedLogicalRelationNode));
  }

  @Test
  void testSerializeLogicalPlanReturnsAlwaysValidJson() throws IOException {
    LogicalPlan notSerializablePlan =
        new LogicalPlan() {
          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public Seq<LogicalPlan> children() {
            return null;
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };
    LogicalPlanSerializer logicalPlanSerializer = new LogicalPlanSerializer();

    final ObjectMapper mapper = new ObjectMapper();
    try {
      mapper.readTree(logicalPlanSerializer.serialize(notSerializablePlan));
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  void testSerializeInsertIntoHadoopPlan()
      throws IOException, InvocationTargetException, IllegalAccessException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();

    HadoopFsRelation hadoopFsRelation =
        new HadoopFsRelation(
            new CatalogFileIndex(
                session,
                CatalogTableTestUtils.getCatalogTable(
                    new TableIdentifier(TEST, Option.apply("db"))),
                100L),
            new StructType(
                new StructField[] {
                  new StructField(NAME, StringType$.MODULE$, false, Metadata.empty())
                }),
            new StructType(
                new StructField[] {
                  new StructField(NAME, StringType$.MODULE$, false, Metadata.empty())
                }),
            Option.empty(),
            new TextFileFormat(),
            new HashMap<>(),
            session);
    LogicalRelation logicalRelation =
        new LogicalRelation(
            hadoopFsRelation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);
    InsertIntoHadoopFsRelationCommand command =
        new InsertIntoHadoopFsRelationCommand(
            new org.apache.hadoop.fs.Path("/tmp"),
            new HashMap<>(),
            false,
            Seq$.MODULE$
                .<Attribute>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            new TextFileFormat(),
            new HashMap<>(),
            logicalRelation,
            SaveMode.Overwrite,
            Option.empty(),
            Option.empty(),
            Seq$.MODULE$.<String>newBuilder().$plus$eq(NAME).result());

    Map<String, Object> commandActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(command), mapTypeReference);
    Map<String, Object> hadoopFSActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedCommandNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "insertintofs-node.json");
    Path expectedHadoopFSNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "hadoopfsrelation-node.json");

    Map<String, Object> expectedCommandNode =
        objectMapper.readValue(expectedCommandNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedHadoopFSNode =
        objectMapper.readValue(expectedHadoopFSNodePath.toFile(), mapTypeReference);

    assertThat(commandActualNode)
        .satisfies(new MatchesMapRecursively(expectedCommandNode, Collections.singleton(EXPR_ID)));
    assertThat(hadoopFSActualNode)
        .satisfies(new MatchesMapRecursively(expectedHadoopFSNode, Collections.singleton(EXPR_ID)));
  }

  @Test
  void testSerializeBigQueryPlan() throws IOException {
    String query = "SELECT date FROM bigquery-public-data.google_analytics_sample.test";
    System.setProperty("GOOGLE_CLOUD_PROJECT", "test_serialization");
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            ImmutableMap.of(
                "query",
                query,
                "dataset",
                "test-dataset",
                "maxparallelism",
                "2",
                "partitionexpirationms",
                "2"),
            ImmutableMap.of(),
            new Configuration(),
            ImmutableMap.of(),
            10,
            SQLConf.get(),
            "",
            Optional.empty(),
            false);

    BigQueryRelation bigQueryRelation =
        new BigQueryRelation(
            config,
            TableInfo.newBuilder(TableId.of("dataset", TEST), new TestTableDefinition()).build(),
            mock(SQLContext.class));

    LogicalRelation logicalRelation =
        new LogicalRelation(
            bigQueryRelation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    InsertIntoDataSourceCommand command =
        new InsertIntoDataSourceCommand(logicalRelation, logicalRelation, false);

    Map<String, Object> commandActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(command), mapTypeReference);
    Map<String, Object> bigqueryActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedCommandNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "insertintods-node.json");
    Path expectedBigQueryRelationNodePath =
        Paths.get(SRC, TEST, RESOURCES, TEST_DATA, SERDE, "bigqueryrelation-node.json");

    Map<String, Object> expectedCommandNode =
        objectMapper.readValue(expectedCommandNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedBigQueryRelationNode =
        objectMapper.readValue(expectedBigQueryRelationNodePath.toFile(), mapTypeReference);

    assertThat(commandActualNode)
        .satisfies(new MatchesMapRecursively(expectedCommandNode, Collections.singleton(EXPR_ID)));
    assertThat(bigqueryActualNode)
        .satisfies(
            new MatchesMapRecursively(
                expectedBigQueryRelationNode, Collections.singleton(EXPR_ID)));
  }

  @Test
  @SneakyThrows
  void testSerializeFiltersFields() {
    LogicalPlan plan =
        new LogicalPlan() {
          public SessionCatalog sessionCatalog =
              new SessionCatalog(mock(ExternalCatalog.class)) {
                public FunctionRegistry functionRegistry = mock(FunctionRegistry.class);
              };

          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public StructType schema() {
            return new StructType(
                new StructField[] {
                  new StructField(
                      "aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
                });
          }

          @Override
          public Seq<LogicalPlan> children() {
            return Seq$.MODULE$.empty();
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };

    assertFalse(logicalPlanSerializer.serialize(plan).contains("functionRegistry"));
    assertFalse(
        logicalPlanSerializer.serialize(plan).contains("Unable to serialize logical plan due to"));
  }

  @Test
  void testSerializeSlicesExcessivePayload() {
    LogicalPlan plan =
        new LogicalPlan() {

          public String okField = "some-field}}}}{{{\"";
          public String longField = RandomStringUtils.random(100000);

          @Override
          public Seq<Attribute> output() {
            return null;
          }

          @Override
          public StructType schema() {
            return null;
          }

          @Override
          public Seq<LogicalPlan> children() {
            return Seq$.MODULE$.empty();
          }

          @Override
          public Object productElement(int n) {
            return null;
          }

          @Override
          public int productArity() {
            return 0;
          }

          @Override
          public boolean canEqual(Object that) {
            return false;
          }
        };

    String serializedPlanString = new LogicalPlanSerializer().serialize(plan);

    assertTrue(serializedPlanString.length() < 51000); // few extra bytes for json encoding
    assertTrue(serializedPlanString.contains("some-field}}}}{{{\\\\\\\""));
    try {
      new ObjectMapper().readTree(serializedPlanString);
    } catch (IOException e) {
      fail(); // not a valid JSON
    }
  }

  @SuppressWarnings("rawtypes")
  static class TestTableDefinition extends TableDefinition {

    @Override
    public TableDefinition.Type getType() {
      return TableDefinition.Type.EXTERNAL;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      return Schema.of(Field.of(NAME, LegacySQLTypeName.STRING));
    }

    @Override
    public TableDefinition.Builder toBuilder() {
      return new TestTableDefinitionBuilder();
    }

    static class TestTableDefinitionBuilder extends TableDefinition.Builder {

      @Override
      public TableDefinition.Builder setType(TableDefinition.Type type) {
        return this;
      }

      @Override
      public TableDefinition.Builder setSchema(Schema schema) {
        return this;
      }

      @Override
      public TableDefinition build() {
        return new TestTableDefinition();
      }
    }
  }
}
