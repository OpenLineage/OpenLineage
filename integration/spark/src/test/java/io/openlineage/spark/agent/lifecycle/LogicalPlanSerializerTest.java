package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
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
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map$;

class LogicalPlanSerializerTest {
  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};

  @Test
  public void testSerializeLogicalPlan() throws IOException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    String jdbcUrl = "jdbc:postgresql://postgreshost:5432/sparkdata";
    String sparkTableName = "my_spark_table";
    JDBCRelation relation =
        new JDBCRelation(
            new StructType(
                new StructField[] {
                  new StructField("name", StringType$.MODULE$, false, Metadata.empty())
                }),
            new Partition[] {},
            new JDBCOptions(
                jdbcUrl,
                sparkTableName,
                Map$.MODULE$
                    .newBuilder()
                    .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                    .result()),
            session);
    LogicalRelation logicalRelation =
        new LogicalRelation(
            relation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
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
    ObjectMapper objectMapper = new ObjectMapper();

    LogicalPlanSerializer logicalPlanSerializer = LogicalPlanSerializer.getInstance();
    Map<String, Object> aggregateActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(aggregate), mapTypeReference);
    Map<String, Object> logicalRelationActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedAggregateNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "aggregate-node.json");
    Path logicalRelationNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "logicalrelation-node.json");

    Map<String, Object> expectedAggregateNode =
        objectMapper.readValue(expectedAggregateNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedLogicalRelationNode =
        objectMapper.readValue(logicalRelationNodePath.toFile(), mapTypeReference);

    assertThat(aggregateActualNode)
        .satisfies(new RecursiveMatcher(expectedAggregateNode, new HashSet<>()));
    assertThat(logicalRelationActualNode)
        .satisfies(new RecursiveMatcher(expectedLogicalRelationNode, new HashSet<>()));
  }

  @Test
  public void testSerializeInsertIntoHadoopPlan() throws IOException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();

    HadoopFsRelation hadoopFsRelation =
        new HadoopFsRelation(
            new CatalogFileIndex(
                session,
                CatalogTable.apply(
                    new TableIdentifier("test", Option.apply("db")),
                    CatalogTableType.MANAGED(),
                    CatalogStorageFormat.empty(),
                    new StructType(
                        new StructField[] {
                          new StructField("name", StringType$.MODULE$, false, Metadata.empty())
                        }),
                    Option.empty(),
                    Seq$.MODULE$.<String>newBuilder().$plus$eq("name").result(),
                    Option.empty(),
                    "",
                    Instant.now().getEpochSecond(),
                    Instant.now().getEpochSecond(),
                    "v1",
                    new HashMap<>(),
                    Option.empty(),
                    Option.empty(),
                    Option.empty(),
                    Seq$.MODULE$.<String>empty(),
                    false,
                    false,
                    new HashMap<>()),
                100L),
            new StructType(
                new StructField[] {
                  new StructField("name", StringType$.MODULE$, false, Metadata.empty())
                }),
            new StructType(
                new StructField[] {
                  new StructField("name", StringType$.MODULE$, false, Metadata.empty())
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
                        "name",
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
                        "name",
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
            Seq$.MODULE$.<String>newBuilder().$plus$eq("name").result());
    ObjectMapper objectMapper = new ObjectMapper();

    LogicalPlanSerializer logicalPlanSerializer = LogicalPlanSerializer.getInstance();
    Map<String, Object> commandActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(command), mapTypeReference);
    Map<String, Object> hadoopFSActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedCommandNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "insertintofs-node.json");
    Path expectedHadoopFSNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "hadoopfsrelation-node.json");

    Map<String, Object> expectedCommandNode =
        objectMapper.readValue(expectedCommandNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedHadoopFSNode =
        objectMapper.readValue(expectedHadoopFSNodePath.toFile(), mapTypeReference);

    assertThat(commandActualNode)
        .satisfies(new RecursiveMatcher(expectedCommandNode, new HashSet<>()));
    assertThat(hadoopFSActualNode)
        .satisfies(new RecursiveMatcher(expectedHadoopFSNode, new HashSet<>()));
  }

  @Test
  public void testSerializeBigQueryPlan() throws IOException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
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
            10,
            SQLConf.get(),
            "",
            Optional.empty());

    BigQueryRelation bigQueryRelation =
        new BigQueryRelation(
            config,
            TableInfo.newBuilder(TableId.of("dataset", "test"), new TestTableDefinition()).build(),
            SQLContext.getOrCreate(session.sparkContext()));

    LogicalRelation logicalRelation =
        new LogicalRelation(
            bigQueryRelation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        "name",
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
    ObjectMapper objectMapper = new ObjectMapper();

    LogicalPlanSerializer logicalPlanSerializer = LogicalPlanSerializer.getInstance();
    Map<String, Object> commandActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(command), mapTypeReference);
    Map<String, Object> hadoopFSActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(logicalRelation), mapTypeReference);

    Path expectedCommandNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "insertintods-node.json");
    Path expectedBigQueryRelationNodePath =
        Paths.get("src", "test", "resources", "test_data", "serde", "bigqueryrelation-node.json");

    Map<String, Object> expectedCommandNode =
        objectMapper.readValue(expectedCommandNodePath.toFile(), mapTypeReference);
    Map<String, Object> expectedBigQueryRelationNode =
        objectMapper.readValue(expectedBigQueryRelationNodePath.toFile(), mapTypeReference);

    assertThat(commandActualNode)
        .satisfies(new RecursiveMatcher(expectedCommandNode, new HashSet<>()));
    assertThat(hadoopFSActualNode)
        .satisfies(new RecursiveMatcher(expectedBigQueryRelationNode, new HashSet<>()));
  }

  class TestTableDefinition extends TableDefinition {
    @Override
    public TableDefinition.Type getType() {
      return TableDefinition.Type.EXTERNAL;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      return Schema.of(Field.of("name", LegacySQLTypeName.STRING));
    }

    @Override
    public TableDefinition.Builder toBuilder() {
      return new TestTableDefinitionBuilder();
    }

    class TestTableDefinitionBuilder extends TableDefinition.Builder {

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
    };
  }
}
