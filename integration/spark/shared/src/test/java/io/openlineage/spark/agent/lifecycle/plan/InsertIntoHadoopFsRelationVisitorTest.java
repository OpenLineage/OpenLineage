/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.FileWriteTargetResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

class InsertIntoHadoopFsRelationVisitorTest {
  private static final String PATH = "abfss://container@account.dfs.core.windows.net/out/files";
  private static final String TARGET_NS = "vendor://host";

  private Optional<FileWriteTargetResolver.Target> target = Optional.empty();
  private InsertIntoHadoopFsRelationVisitor visitor;
  private OpenLineageContext context;

  @BeforeEach
  void setup() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(session.sparkContext()).thenReturn(sparkContext);

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    OpenLineageContext context =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
    // a vendor resolver that returns whatever target the test sets
    context.getFileWriteTargetResolvers().add(command -> target);
    this.context = context;
    visitor = new InsertIntoHadoopFsRelationVisitor(context);
  }

  @Test
  void testWriteIsReportedAsItsPath() {
    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(new HashMap<>()));

    assertThat(outputs).hasSize(1);
    OpenLineage.OutputDataset output = outputs.get(0);
    assertThat(output.getNamespace()).isEqualTo("abfss://container@account.dfs.core.windows.net");
    assertThat(output.getName()).isEqualTo("out/files");
    assertThat(output.getFacets().getLifecycleStateChange().getLifecycleStateChange())
        .isEqualTo(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  @Test
  void testVendorTargetReplacesThePath() {
    target =
        Optional.of(
            FileWriteTargetResolver.Target.of(new DatasetIdentifier("db.schema.table", TARGET_NS)));

    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(new HashMap<>()));

    assertThat(outputs).hasSize(1);
    OpenLineage.OutputDataset output = outputs.get(0);
    assertThat(output.getNamespace()).isEqualTo(TARGET_NS);
    assertThat(output.getName()).isEqualTo("db.schema.table");
    // the save mode applies to the intermediate files, not to the vendor's output
    assertThat(output.getFacets().getLifecycleStateChange()).isNull();
    assertThat(visitor.jobNameSuffix(command(new HashMap<>()))).contains("db.schema.table");
  }

  @Test
  void testVendorCanReportNoOutput() {
    target = Optional.of(FileWriteTargetResolver.Target.none());

    assertThat(visitor.apply(command(new HashMap<>()))).isEmpty();
  }

  @Test
  void testFailingVendorResolverReportsThePath() {
    context.getFileWriteTargetResolvers().clear();
    context
        .getFileWriteTargetResolvers()
        .add(
            command -> {
              throw new IllegalStateException("broken vendor");
            });

    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(new HashMap<>()));

    assertThat(outputs)
        .singleElement()
        .satisfies(o -> assertThat(o.getName()).isEqualTo("out/files"));
    assertThat(visitor.jobNameSuffix(command(new HashMap<>()))).isPresent();
  }

  @Test
  void testFailingVendorFacetBuilderKeepsTheDataset() {
    context
        .getDatasetIdentityFacetBuilders()
        .add(
            (identifier, facets) -> {
              throw new NoSuchMethodError("built against another Spark version");
            });

    assertThat(visitor.apply(command(new HashMap<>())))
        .singleElement()
        .satisfies(o -> assertThat(o.getName()).isEqualTo("out/files"));
  }

  private static InsertIntoHadoopFsRelationCommand command(Map<String, String> options) {
    LogicalPlan query = mock(LogicalPlan.class);
    when(query.schema())
        .thenReturn(
            new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.StringType));
    return new InsertIntoHadoopFsRelationCommand(
        new Path(PATH),
        ScalaConversionUtils.asScalaMapEmpty(),
        false,
        ScalaConversionUtils.asScalaSeqEmpty(),
        Option.empty(),
        null,
        ScalaConversionUtils.fromJavaMap(options),
        query,
        SaveMode.Overwrite,
        Option.empty(),
        Option.empty(),
        ScalaConversionUtils.<String>asScalaSeqEmpty());
  }
}
