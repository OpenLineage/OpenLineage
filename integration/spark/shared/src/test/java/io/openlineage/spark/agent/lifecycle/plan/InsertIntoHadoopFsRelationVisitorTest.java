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
          + "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com";

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
    sparkConf.set("trident.tenant.id", "68a4b712-9eec-4ca0-a2d3-6a96d2cf4f9c");
    sparkConf.set("trident.workspace.id", "d1f6473b-9f7a-4352-940b-a8d74461d3fc");
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
                      "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com");
              assertThat(symlink.getName())
                  .isEqualTo("d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/probe_dw/a_write");
              assertThat(symlink.getType()).isEqualTo("LOCATION");
            });
  }

  @Test
  void testConnectorWriteHasOneLakeFacet() {
    // Fabric puts the workspace's name only in the session's settings
    runtimeConf.put("trident.workspace.name", "sales-analytics");

    OpenLineage.FabricOneLakeDatasetFacet facet =
        connectorWriteOutput().getFacets().getFabric_onelake();

    assertThat(facet.getItemType()).isEqualTo("Warehouse");
    assertThat(facet.getItemName()).isEqualTo("wh_primary");
    assertThat(facet.getWorkspaceId()).isEqualTo("d1f6473b-9f7a-4352-940b-a8d74461d3fc");
    assertThat(facet.getWorkspaceName()).isEqualTo("sales-analytics");
    assertThat(facet.getItemId()).isEqualTo("d75873bd-716b-4378-a37e-864c76fc9b3b");
    assertThat(facet.getLocation())
        .isEqualTo(
            "abfss://d1f6473b-9f7a-4352-940b-a8d74461d3fc@onelake.dfs.fabric.microsoft.com/"
                + "d75873bd-716b-4378-a37e-864c76fc9b3b/Tables/probe_dw/a_write");
    assertThat(facet.getTableId()).isNull();
  }

  @Test
  void testConnectorWriteToAnotherWorkspaceHasNoWorkspaceName() {
    runtimeConf.put("trident.workspace.name", "sales-analytics");
    Map<String, String> options = observedOptions();
    options.put("WorkspaceId", "45ff911e-5e75-428a-a618-52958fcc7604");

    OpenLineage.FabricOneLakeDatasetFacet facet =
        visitor.apply(command(options)).get(0).getFacets().getFabric_onelake();

    // the address is built for the other workspace, so the session's workspace name doesn't apply
    assertThat(facet.getWorkspaceId()).isEqualTo("45ff911e-5e75-428a-a618-52958fcc7604");
    assertThat(facet.getWorkspaceName()).isNull();
    assertThat(facet.getItemId()).isEqualTo("d75873bd-716b-4378-a37e-864c76fc9b3b");
  }

  @Test
  void testConnectorWriteWithoutWarehouseIdHasNoLocation() {
    Map<String, String> options = observedOptions();
    options.remove("DatawarehouseId");

    OpenLineage.FabricOneLakeDatasetFacet facet =
        visitor.apply(command(options)).get(0).getFacets().getFabric_onelake();

    assertThat(facet.getItemName()).isEqualTo("wh_primary");
    assertThat(facet.getItemId()).isNull();
    assertThat(facet.getLocation()).isNull();
  }

  private OpenLineage.OutputDataset connectorWriteOutput() {
    List<OpenLineage.OutputDataset> outputs = visitor.apply(command(observedOptions()));
    assertThat(outputs).hasSize(1);
    return outputs.get(0);
  }

  // the options the Fabric connector's first write step has on a real workspace, with made-up IDs
  private static Map<String, String> observedOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("WorkspaceId", "d1f6473b-9f7a-4352-940b-a8d74461d3fc");
    options.put("DatawarehouseId", "d75873bd-716b-4378-a37e-864c76fc9b3b");
    options.put("three-part_sql_table_or_view_name", "wh_primary.probe_dw.a_write");
    options.put("path", STAGING_PATH);
    return options;
  }

  @Test
  void testConnectorWriteUsesEndpointOverride() {
    runtimeConf.put(
        "spark.datawarehouse.wh_primary.sqlendpoint",
        "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com,1433");
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
        "45jm47djf4je5afhvapdos5fae-gakpgz3u6twe7nemz77to4q2ti-jh0bi.zcf.datawarehouse.fabric.microsoft.com");

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
    assertThat(output.getFacets().getFabric_onelake()).isNull();
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
