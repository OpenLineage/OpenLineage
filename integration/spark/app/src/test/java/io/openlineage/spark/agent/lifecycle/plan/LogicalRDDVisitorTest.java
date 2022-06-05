/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(SparkAgentTestExtension.class)
class LogicalRDDVisitorTest {

  JobConf jobConf;
  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  public void setUp() {
    when(context.getOpenLineage())
        .thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @AfterEach
  public void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @Test
  public void testApply(@TempDir Path tmpDir) {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    LogicalRDDVisitor visitor =
        new LogicalRDDVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("anInt", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    jobConf = new JobConf();
    FileInputFormat.addInputPath(jobConf, new org.apache.hadoop.fs.Path("file://" + tmpDir));
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
    List<OpenLineage.Dataset> datasets = visitor.apply(logicalRDD);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", tmpDir.toString())
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
