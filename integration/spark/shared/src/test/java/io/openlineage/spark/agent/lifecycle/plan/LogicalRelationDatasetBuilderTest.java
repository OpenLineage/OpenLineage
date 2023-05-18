/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.collection.immutable.Map$;

class LogicalRelationDatasetBuilderTest {

  SparkSession session = mock(SparkSession.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  LogicalRelationDatasetBuilder builder =
      new LogicalRelationDatasetBuilder(
          openLineageContext, DatasetFactory.output(openLineageContext), false);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    when(openLineageContext.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @ParameterizedTest
  @CsvSource({
    "jdbc:postgresql://postgreshost:5432/sparkdata,postgres://postgreshost:5432/sparkdata",
    "jdbc:oracle:oci8:@sparkdata,oracle:oci8:@sparkdata",
    "jdbc:oracle:thin@sparkdata:1521:orcl,oracle:thin@sparkdata:1521:orcl",
    "jdbc:mysql://localhost/sparkdata,mysql://localhost/sparkdata"
  })
  void testApply(String connectionUri, String targetUri) {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    String sparkTableName = "my_spark_table";
    JDBCRelation relation =
        new JDBCRelation(
            new StructType(
                new StructField[] {new StructField("name", StringType$.MODULE$, false, null)}),
            new Partition[] {},
            new JDBCOptions(
                connectionUri,
                sparkTableName,
                Map$.MODULE$
                    .<String, String>newBuilder()
                    .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                    .result()),
            session);
    QueryExecution qe = mock(QueryExecution.class);
    when(qe.optimizedPlan())
        .thenReturn(
            new Project(
                Seq$.MODULE$.<String>empty(),
                new LogicalRelation(
                    relation,
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
                    false)));
    OpenLineageContext context =
        OpenLineageContext.builder()
            .sparkContext(mock(SparkContext.class))
            .openLineage(openLineage)
            .queryExecution(qe)
            .build();
    LogicalRelationDatasetBuilder visitor =
        new LogicalRelationDatasetBuilder<>(
            context, DatasetFactory.output(openLineageContext), true);
    List<OutputDataset> datasets =
        visitor.apply(new SparkListenerJobStart(1, 1, Seq$.MODULE$.empty(), null));
    assertEquals(1, datasets.size());
    OutputDataset ds = datasets.get(0);
    assertEquals(targetUri, ds.getNamespace());
    assertEquals(sparkTableName, ds.getName());
    assertEquals(URI.create(targetUri), ds.getFacets().getDataSource().getUri());
    assertEquals(targetUri, ds.getFacets().getDataSource().getName());
  }

  @Test
  void testApplyForHadoopFsRelation() {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    LogicalRelation logicalRelation = mock(LogicalRelation.class);
    Configuration hadoopConfig = mock(Configuration.class);
    SparkContext sparkContext = mock(SparkContext.class);
    FileIndex fileIndex = mock(FileIndex.class);
    SessionState sessionState = mock(SessionState.class);
    Path p1 = new Path("/tmp/path1");
    Path p2 = new Path("/tmp/path2");

    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(session.sessionState()).thenReturn(sessionState);
    when(sessionState.newHadoopConfWithOptions(any())).thenReturn(hadoopConfig);
    when(hadoopFsRelation.location()).thenReturn(fileIndex);
    when(fileIndex.rootPaths())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(p1, p2))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(PlanUtils.class)) {
      when(PlanUtils.getDirectoryPath(p1, hadoopConfig)).thenReturn(new Path("/tmp"));
      when(PlanUtils.getDirectoryPath(p2, hadoopConfig)).thenReturn(new Path("/tmp"));

      List<OpenLineage.Dataset> datasets = builder.apply(logicalRelation);
      assertEquals(1, datasets.size());
      OpenLineage.Dataset ds = datasets.get(0);
      assertEquals("/tmp", ds.getName());
    }
  }

  @Test
  void testApplyForSingleFileHadoopFsRelation() throws IOException, URISyntaxException {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    LogicalRelation logicalRelation = mock(LogicalRelation.class);
    Configuration hadoopConfig = mock(Configuration.class);
    SparkContext sparkContext = mock(SparkContext.class);
    FileIndex fileIndex = mock(FileIndex.class);
    SessionState sessionState = mock(SessionState.class);
    Path p = mock(Path.class);
    FileSystem fileSystem = mock(FileSystem.class);
    when(p.getFileSystem(hadoopConfig)).thenReturn(fileSystem);
    when(p.toUri()).thenReturn(new URI("/tmp/path.csv"));
    when(fileSystem.isFile(p)).thenReturn(true);

    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(openLineageContext.getSparkSession()).thenReturn(Optional.of(session));
    when(session.sessionState()).thenReturn(sessionState);
    when(sessionState.newHadoopConfWithOptions(any())).thenReturn(hadoopConfig);
    when(hadoopFsRelation.location()).thenReturn(fileIndex);
    when(fileIndex.rootPaths())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(p))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(PlanUtils.class)) {
      List<OpenLineage.Dataset> datasets = builder.apply(logicalRelation);
      assertEquals(1, datasets.size());
      OpenLineage.Dataset ds = datasets.get(0);
      assertEquals("/tmp/path.csv", ds.getName());
    }
  }
}
