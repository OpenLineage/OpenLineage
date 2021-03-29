package marquez.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import marquez.spark.agent.client.LineageEvent.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.collection.immutable.Map$;

class JDBCRelationVisitorTest {

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
    JDBCRelationVisitor visitor = new JDBCRelationVisitor(null);
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
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
}
