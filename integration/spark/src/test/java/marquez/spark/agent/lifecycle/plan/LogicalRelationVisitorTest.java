package marquez.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import marquez.spark.agent.SparkAgentTestExtension;
import marquez.spark.agent.client.LineageEvent.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.streaming.ConsoleRelation;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map$;

@ExtendWith(SparkAgentTestExtension.class)
class LogicalRelationVisitorTest {
  @AfterEach
  public void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
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
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    LogicalRelationVisitor visitor =
        new LogicalRelationVisitor(session.sparkContext(), "testnamespace");
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
                    .newBuilder()
                    .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                    .result()),
            session);
    List<Dataset> datasets =
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
    Dataset ds = datasets.get(0);
    assertEquals(connectionUri, ds.getNamespace());
    assertEquals(sparkTableName, ds.getName());
    assertEquals(connectionUri, ds.getFacets().getDataSource().getUri());
    assertEquals(connectionUri, ds.getFacets().getDataSource().getName());
  }

  @Test
  void testApplyUnknownRelation() {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    LogicalRelationVisitor visitor =
        new LogicalRelationVisitor(session.sparkContext(), "testnamespace");
    org.apache.spark.sql.Dataset<Row> dataFrame =
        session.createDataFrame(
            Arrays.asList(new GenericRow(new Object[] {1, "hello", 5.4})),
            new StructType(
                new StructField[] {
                  new StructField(
                      "num", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
                  new StructField(
                      "word", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
                  new StructField("fl", FloatType$.MODULE$, false, new Metadata(new HashMap<>()))
                }));
    ConsoleRelation consoleRelation = new ConsoleRelation(session.sqlContext(), dataFrame);
    LogicalRelation logicalRelation =
        new LogicalRelation(
            consoleRelation, consoleRelation.schema().toAttributes(), Option.empty(), false);
    assertTrue(visitor.isDefinedAt(logicalRelation));
    List<Dataset> datasets = visitor.apply(logicalRelation);
    Assertions.assertThat(datasets)
        .isNotEmpty()
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("name", "ConsoleRelation_struct<num:int,word:string,fl:float>")
        .extracting("facets")
        .extracting("description")
        .asString()
        .startsWith("Relation[num#6,word#7,fl#8] ConsoleRelation(");
  }
}
