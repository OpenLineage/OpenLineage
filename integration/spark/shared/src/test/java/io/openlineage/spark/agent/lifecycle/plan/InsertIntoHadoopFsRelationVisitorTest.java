/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class InsertIntoHadoopFsRelationVisitorTest {
  private static final String STAGING_PATH =
      "abfss://e5c0cf65-0000-0000-0000-000000000000@spark1triprodcus.dfs.core.windows.net"
          + "/user/trusted-service-user/a_write5f15";
  private static final String NAMESPACE =
      "fabric-warehouse://"
          + "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com";

  private final SparkConf sparkConf = new SparkConf();
  private final Map<String, String> runtimeConf = new HashMap<>();
  private InsertIntoHadoopFsRelationVisitor visitor;

  @BeforeEach
  void setup() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(session.sparkContext()).thenReturn(sparkContext);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(conf.getOption(anyString()))
        .thenAnswer(invocation -> Option.apply(runtimeConf.get(invocation.<String>getArgument(0))));
    when(session.conf()).thenReturn(conf);

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
    visitor = new InsertIntoHadoopFsRelationVisitor(context);

    // Fabric sets these Spark settings in every notebook
    sparkConf.set("trident.tenant.id", "cc0b82f3-7c2e-400b-aec3-40a3d720505b");
    sparkConf.set("trident.workspace.id", "8b4cb800-a044-43ce-8f76-3356f709b468");
  }

  @Test
  void testConnectorWriteEmitsWarehouseTable() {
    OpenLineage.OutputDataset output = connectorWriteOutput();

    assertThat(output.getNamespace()).isEqualTo(NAMESPACE);
    assertThat(output.getName()).isEqualTo("wh_primary.probe_dw.a_write");
    assertThat(visitor.jobNameSuffix(command(observedOptions())))
        .hasValue("wh_primary.probe_dw.a_write");
  }

  @Test
  void testConnectorWriteHasNoStagingFacets() {
    OpenLineage.OutputDataset output = connectorWriteOutput();

    assertThat(output.getFacets().getLifecycleStateChange()).isNull();
    assertThat(output.getFacets().getDataSource().getName()).isEqualTo(NAMESPACE);
    assertThat(output.getFacets().getSchema().getFields()).hasSize(2);
  }

  @Test
  void testConnectorWriteHasOneLakeLocation() {
    OpenLineage.OutputDataset output = connectorWriteOutput();

    assertThat(output.getFacets().getSymlinks().getIdentifiers())
        .singleElement()
        .satisfies(
            symlink -> {
              assertThat(symlink.getNamespace())
                  .isEqualTo(
                      "abfss://8b4cb800-a044-43ce-8f76-3356f709b468@onelake.dfs.fabric.microsoft.com");
              assertThat(symlink.getName())
                  .isEqualTo("0fd9a0c2-ee93-46af-aaea-a45c8649ed9d/Tables/probe_dw/a_write");
              assertThat(symlink.getType()).isEqualTo("LOCATION");
            });
  }

  private OpenLineage.OutputDataset connectorWriteOutput() {
    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(observedOptions()));
    assertThat(outputs).hasSize(1);
    return outputs.get(0);
  }

  // the options seen on the Fabric connector's first write step, on a real workspace
  private static Map<String, String> observedOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("WorkspaceId", "8b4cb800-a044-43ce-8f76-3356f709b468");
    options.put("DatawarehouseId", "0fd9a0c2-ee93-46af-aaea-a45c8649ed9d");
    options.put("three-part_sql_table_or_view_name", "wh_primary.probe_dw.a_write");
    options.put("path", STAGING_PATH);
    return options;
  }

  @Test
  void testConnectorWriteUsesEndpointOverride() {
    runtimeConf.put(
        "spark.datawarehouse.wh_primary.sqlendpoint",
        "6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com,1433");
    sparkConf.remove("trident.tenant.id");
    Map<String, String> options = new HashMap<>();
    options.put("three-part_sql_table_or_view_name", "wh_primary.probe_dw.a_write");
    options.put("path", STAGING_PATH);

    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(options));

    assertThat(outputs)
        .singleElement()
        .satisfies(o -> assertThat(o.getNamespace()).isEqualTo(NAMESPACE));
  }

  @Test
  void testConnectorWriteWithGuestAddressSettingEmitsNoOutput() {
    // the tenant and workspace in the Spark settings could build an address, but must not be used
    runtimeConf.put(
        "spark.datawarehouse.wh_primary.sqlendpoint",
        "qvrmbuxie7we7glrekxgy6npqu-6xgyei3x2xiejip4iime6knh5m-jh0bi.zcf.datawarehouse.fabric.microsoft.com");

    assertThat(visitor.apply(command(observedOptions()))).isEmpty();
  }

  @Test
  void testConnectorWriteWithoutEndpointEmitsNoOutput() {
    sparkConf.remove("trident.tenant.id");
    Map<String, String> options = new HashMap<>();
    options.put("three-part_sql_table_or_view_name", "wh_primary.probe_dw.a_write");
    options.put("path", STAGING_PATH);

    assertThat(visitor.apply(command(options))).isEmpty();
  }

  @Test
  void testPlainWriteIsUnchanged() {
    Map<String, String> options = new HashMap<>();
    options.put("path", STAGING_PATH);

    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(options));

    assertThat(outputs).hasSize(1);
    OpenLineage.OutputDataset output = outputs.get(0);
    assertThat(output.getNamespace())
        .isEqualTo(
            "abfss://e5c0cf65-0000-0000-0000-000000000000@spark1triprodcus.dfs.core.windows.net");
    assertThat(output.getName()).isEqualTo("user/trusted-service-user/a_write5f15");
    assertThat(output.getFacets().getLifecycleStateChange().getLifecycleStateChange())
        .isEqualTo(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE);
  }

  private static InsertIntoHadoopFsRelationCommand command(Map<String, String> options) {
    LogicalPlan query = mock(LogicalPlan.class);
    when(query.schema())
        .thenReturn(
            new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.StringType));
    return new InsertIntoHadoopFsRelationCommand(
        new Path(STAGING_PATH),
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
