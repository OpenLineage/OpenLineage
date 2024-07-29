/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.Partition;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.execution.datasources.CatalogFileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.text.TextFileFormat;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import scala.Option;
import scala.collection.immutable.HashMap;

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
        ScalaConversionUtils.fromJavaMap(
            Collections.singletonMap("driver", Driver.class.getName()));
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
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
            Option.empty(),
            false);
    Aggregate aggregate =
        new Aggregate(
            ScalaConversionUtils.asScalaSeqEmpty(),
            ScalaConversionUtils.asScalaSeqEmpty(),
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
  void testSerializeInsertIntoHadoopPlan() throws IOException {
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
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
            Option.empty(),
            false);
    InsertIntoHadoopFsRelationCommand command =
        new InsertIntoHadoopFsRelationCommand(
            new org.apache.hadoop.fs.Path("/tmp"),
            new HashMap<>(),
            false,
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        NAME,
                        StringType$.MODULE$,
                        false,
                        Metadata.empty(),
                        ExprId.apply(1L),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
            Option.empty(),
            new TextFileFormat(),
            new HashMap<>(),
            logicalRelation,
            SaveMode.Overwrite,
            Option.empty(),
            Option.empty(),
            ScalaConversionUtils.fromList(Collections.singletonList(NAME)));

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
}
