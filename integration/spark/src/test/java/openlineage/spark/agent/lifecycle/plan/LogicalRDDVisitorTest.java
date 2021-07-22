package openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import openlineage.spark.agent.SparkAgentTestExtension;
import openlineage.spark.agent.client.LineageEvent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition$;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Seq$;

@ExtendWith(SparkAgentTestExtension.class)
class LogicalRDDVisitorTest {

  private JobConf jobConf;

  @AfterEach
  public void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @Test
  public void testApply(@TempDir Path tmpDir) {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    LogicalRDDVisitor visitor = new LogicalRDDVisitor();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("anInt", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    jobConf = new JobConf();
    FileInputFormat.addInputPath(jobConf, new org.apache.hadoop.fs.Path("file:///path/to/data/"));
    RDD<InternalRow> hadoopRdd =
        new HadoopRDD<>(
                session.sparkContext(),
                jobConf,
                TextInputFormat.class,
                LongWritable.class,
                Text.class,
                1)
            .toJavaRDD()
            .map(t -> (InternalRow) new GenericInternalRow(new Object[] {t._2.toString()}))
            .rdd();

    LogicalRDD logicalRDD =
        new LogicalRDD(
            ScalaConversionUtils.fromSeq(schema.toAttributes()).stream()
                .map(AttributeReference::toAttribute)
                .collect(ScalaConversionUtils.toSeq()),
            hadoopRdd,
            SinglePartition$.MODULE$,
            Seq$.MODULE$.<SortOrder>empty(),
            false,
            session);
    assertThat(visitor.isDefinedAt(logicalRDD)).isTrue();
    List<LineageEvent.Dataset> datasets = visitor.apply(logicalRDD);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/path/to/data")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
