/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.postgresql.Driver;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.HashMap;

// This test is disabled for Spark 4.x versions, as the LogicalPlan constructor has changed
@DisabledIfSystemProperty(named = "spark.version", matches = "([4].*)")
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
  void testSerializeLogicalPlan()
      throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
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

    LogicalRelation instance;
    Aggregate instanceAggregate;
    Class<?> logicalRelation =
        Class.forName("org.apache.spark.sql.execution.datasources.LogicalRelation");
    Class<?> aggregateInstance =
        Class.forName("org.apache.spark.sql.catalyst.plans.logical.Aggregate");

    Seq<AttributeReference> output =
        ScalaConversionUtils.fromList(
            Collections.singletonList(
                new AttributeReference(
                    NAME,
                    StringType$.MODULE$,
                    false,
                    Metadata.empty(),
                    ExprId.apply(1L),
                    ScalaConversionUtils.asScalaSeqEmpty())));

    Constructor<?>[] constructors = logicalRelation.getDeclaredConstructors();
    Constructor<?> constructor = constructors[0];

    Constructor<?>[] aggregateConstructors = aggregateInstance.getDeclaredConstructors();
    Constructor<?> aggregatConstructor = aggregateConstructors[0];

    if (System.getProperty("spark.version").startsWith("4")) {
      Object[] paramsVersion4 = new Object[] {relation, output, Option.empty(), false, null};
      instance = (LogicalRelation) constructor.newInstance(paramsVersion4);

      Object[] aggregateParams =
          new Object[] {
            ScalaConversionUtils.asScalaSeqEmpty(),
            ScalaConversionUtils.asScalaSeqEmpty(),
            instance,
            null
          };
      instanceAggregate = (Aggregate) aggregatConstructor.newInstance(aggregateParams);
    } else {
      Object[] paramsVersion3 = new Object[] {relation, output, Option.empty(), false};
      instance = (LogicalRelation) constructor.newInstance(paramsVersion3);

      Object[] aggregateParams =
          new Object[] {
            ScalaConversionUtils.asScalaSeqEmpty(), ScalaConversionUtils.asScalaSeqEmpty(), instance
          };
      instanceAggregate = (Aggregate) aggregatConstructor.newInstance(aggregateParams);
    }

    Map<String, Object> aggregateActualNode =
        objectMapper.readValue(
            logicalPlanSerializer.serialize(instanceAggregate), mapTypeReference);
    Map<String, Object> logicalRelationActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(instance), mapTypeReference);

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
  void testSerializeInsertIntoHadoopPlan()
      throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
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

    LogicalRelation instance;
    Class<?> logicalRelation =
        Class.forName("org.apache.spark.sql.execution.datasources.LogicalRelation");
    Seq<AttributeReference> output =
        ScalaConversionUtils.fromList(
            Collections.singletonList(
                new AttributeReference(
                    NAME,
                    StringType$.MODULE$,
                    false,
                    Metadata.empty(),
                    ExprId.apply(1L),
                    ScalaConversionUtils.asScalaSeqEmpty())));

    Constructor<?>[] constructors = logicalRelation.getDeclaredConstructors();
    Constructor<?> constructor = constructors[0];

    if (System.getProperty("spark.version").startsWith("4")) {
      Object[] paramsVersion4 =
          new Object[] {hadoopFsRelation, output, Option.empty(), false, null};
      instance = (LogicalRelation) constructor.newInstance(paramsVersion4);
    } else {
      Object[] paramsVersion3 = new Object[] {hadoopFsRelation, output, Option.empty(), false};
      instance = (LogicalRelation) constructor.newInstance(paramsVersion3);
    }

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
            instance,
            SaveMode.Overwrite,
            Option.empty(),
            Option.empty(),
            ScalaConversionUtils.fromList(Collections.singletonList(NAME)));

    Map<String, Object> commandActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(command), mapTypeReference);
    Map<String, Object> hadoopFSActualNode =
        objectMapper.readValue(logicalPlanSerializer.serialize(instance), mapTypeReference);

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
