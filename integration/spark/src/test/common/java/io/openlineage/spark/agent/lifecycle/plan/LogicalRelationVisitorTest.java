package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.collection.immutable.Map$;

class LogicalRelationVisitorTest {

  SparkSession session = mock(SparkSession.class);
  OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
  LogicalRelationVisitor visitor =
      new LogicalRelationVisitor(
          openLineageContext,
          DatasetFactory.output(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)),
          false);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "postgresql://postgreshost:5432/sparkdata",
        "jdbc:oracle:oci8:@sparkdata",
        "jdbc:oracle:thin@sparkdata:1521:orcl",
        "mysql://localhost/sparkdata"
      })
  void testApply(String connectionUri) {
    String jdbcUrl = "jdbc:" + connectionUri;
    String sparkTableName = "my_spark_table";
    JDBCRelation relation =
        new JDBCRelation(
            new StructType(
                new StructField[] {new StructField("name", StringType$.MODULE$, false, null)}),
            new Partition[] {},
            new JDBCOptions(
                jdbcUrl,
                sparkTableName,
                Map$.MODULE$
                    .<String, String>newBuilder()
                    .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                    .result()),
            session);
    List<OpenLineage.Dataset> datasets =
        visitor.apply(
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
                false));
    assertEquals(1, datasets.size());
    OpenLineage.Dataset ds = datasets.get(0);
    assertEquals(connectionUri, ds.getNamespace());
    assertEquals(sparkTableName, ds.getName());
    assertEquals(URI.create(connectionUri), ds.getFacets().getDataSource().getUri());
    assertEquals(connectionUri, ds.getFacets().getDataSource().getName());
  }

  @Test
  void testApplyForHadoopFsRelation() {
    HadoopFsRelation hadoopFsRelation = mock(HadoopFsRelation.class);
    LogicalRelation logicalRelation = mock(LogicalRelation.class);
    Configuration hadoopConfig = mock(Configuration.class);
    SparkContext sparkContext = mock(SparkContext.class);
    FileIndex fileIndex = mock(FileIndex.class);
    Path p1 = new Path("/tmp/path1");
    Path p2 = new Path("/tmp/path2");

    when(logicalRelation.relation()).thenReturn(hadoopFsRelation);
    when(openLineageContext.getSparkContext()).thenReturn(sparkContext);
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConfig);
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

      List<OpenLineage.Dataset> datasets = visitor.apply(logicalRelation);
      assertEquals(1, datasets.size());
      OpenLineage.Dataset ds = datasets.get(0);
      assertEquals("/tmp", ds.getName());
    }
  }
}
