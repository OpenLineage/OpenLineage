/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.client.utils.OneLakeLocation;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.DatasetIdentifierResolver;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import io.openlineage.sql.DbTableMeta;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap$;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class OneLakeFacetUtilsTest {
  private static final String WORKSPACE = "d1f6473b-9f7a-4352-940b-a8d74461d3fc";
  private static final String OTHER_WORKSPACE = "45ff911e-5e75-428a-a618-52958fcc7604";
  private static final String WAREHOUSE = "d75873bd-716b-4378-a37e-864c76fc9b3b";
  private static final String HOST =
      "ck32i2hmt2qeziwtnklnft2ptq-hnd7nul2t5jehfalvdluiyot7q.datawarehouse.fabric.microsoft.com";
  private static final String NAMESPACE = "fabric-warehouse://" + HOST;

  private final Map<String, String> runtimeConf = new HashMap<>();
  private final Configuration hadoopConf = new Configuration(false);
  private final SparkOpenLineageConfig config = new SparkOpenLineageConfig();
  private OpenLineageContext context;

  @BeforeEach
  void setup() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(new SparkConf(false));
    when(sparkContext.hadoopConfiguration()).thenReturn(hadoopConf);
    when(session.sparkContext()).thenReturn(sparkContext);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(conf.getOption(anyString()))
        .thenAnswer(invocation -> Option.apply(runtimeConf.get(invocation.<String>getArgument(0))));
    when(session.conf()).thenReturn(conf);
    context =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .build();
  }

  @Test
  void testWarehouseTableRead() {
    // the identifier of spark.read.synapsesql("wh_primary.raw.orders") with DatawarehouseId
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:sqlserver://;servername=" + HOST + ";databaseName=wh_primary");
    options.put("dbtable", "raw.orders");
    options.put("driver", "org.postgresql.Driver");
    options.put("DatawarehouseId", WAREHOUSE);
    DatasetIdentifier identifier =
        JdbcSparkUtils.getDatasetIdentifier(
            new JDBCOptions(
                CaseInsensitiveMap$.MODULE$.apply(ScalaConversionUtils.fromJavaMap(options))),
            new DbTableMeta(null, "raw", "orders"));

    OpenLineage.InputDataset input =
        DatasetFactory.input(context).sparkDatasetBuilder().dataset(identifier).build();

    OpenLineage.FabricOneLakeDatasetFacet facet = input.getFacets().getFabric_onelake();
    assertThat(facet.getItemType()).isEqualTo("Warehouse");
    assertThat(facet.getItemName()).isEqualTo("wh_primary");
    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(facet.getItemId()).isEqualTo(WAREHOUSE);
    assertThat(facet.getLocation())
        .isEqualTo(
            "abfss://"
                + WORKSPACE
                + "@onelake.dfs.fabric.microsoft.com/"
                + WAREHOUSE
                + "/Tables/raw/orders");
    // outside Fabric there's no workspace name to add
    assertThat(facet.getWorkspaceName()).isNull();
  }

  @Test
  void testFacetUsesSymlinksSetThroughTheSymlinksFacet() {
    // a builder that sets the LOCATION symlink as a facet, not through dataset(DatasetIdentifier)
    String location = WAREHOUSE + "/Tables/raw/orders";
    String oneLake = "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com";
    OpenLineage openLineage = context.getOpenLineage();
    OpenLineage.SymlinksDatasetFacet symlinks =
        openLineage
            .newSymlinksDatasetFacetBuilder()
            .identifiers(
                Collections.singletonList(
                    openLineage
                        .newSymlinksDatasetFacetIdentifiersBuilder()
                        .name(location)
                        .namespace(oneLake)
                        .type("LOCATION")
                        .build()))
            .build();

    for (OpenLineage.Dataset dataset :
        new OpenLineage.Dataset[] {
          DatasetFactory.input(context)
              .sparkDatasetBuilder()
              .dataset("wh_primary.raw.orders", NAMESPACE)
              .symlinks(symlinks)
              .build(),
          DatasetFactory.output(context)
              .sparkDatasetBuilder()
              .dataset("wh_primary.raw.orders", NAMESPACE)
              .symlink(symlinks)
              .build()
        }) {
      OpenLineage.FabricOneLakeDatasetFacet facet = dataset.getFacets().getFabric_onelake();
      assertThat(facet.getItemId()).isEqualTo(WAREHOUSE);
      assertThat(facet.getLocation()).isEqualTo(oneLake + "/" + location);
    }
  }

  @Test
  void testReplacedDatasetDropsThePreviousSymlinks() {
    String location = WAREHOUSE + "/Tables/raw/orders";
    String oneLake = "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com";
    DatasetIdentifier first =
        new DatasetIdentifier("wh_primary.raw.orders", NAMESPACE)
            .withSymlink(location, oneLake, DatasetIdentifier.SymlinkType.LOCATION);

    OpenLineage.InputDataset input =
        DatasetFactory.input(context)
            .sparkDatasetBuilder()
            .dataset(first)
            .dataset(new DatasetIdentifier("wh_other.raw.orders", NAMESPACE))
            .build();

    assertThat(input.getFacets().getSymlinks()).isNull();
    OpenLineage.FabricOneLakeDatasetFacet facet = input.getFacets().getFabric_onelake();
    assertThat(facet.getItemName()).isEqualTo("wh_other");
    assertThat(facet.getItemId()).isNull();
    assertThat(facet.getLocation()).isNull();
  }

  @Test
  void testOneLakePathGetsTheFacetFromThePath() {
    String path = WAREHOUSE + "/Tables/raw/orders";
    String oneLake = "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com";

    OpenLineage.FabricOneLakeDatasetFacet facet =
        DatasetFactory.input(context)
            .sparkDatasetBuilder()
            .dataset(path, oneLake)
            .build()
            .getFacets()
            .getFabric_onelake();

    assertThat(facet.getWorkspaceId()).isEqualTo(WORKSPACE);
    assertThat(facet.getItemId()).isEqualTo(WAREHOUSE);
    assertThat(facet.getLocation()).isEqualTo(oneLake + "/" + path);
    // the path doesn't say what the item is
    assertThat(facet.getItemType()).isNull();
    assertThat(facet.getItemName()).isNull();
  }

  @Test
  void testFabricVendorNamesTheDefaultLakehouse() {
    String lakehouse = "c7a14054-60a7-4121-8fb7-252d16f687eb";
    String oneLake = "abfss://" + WORKSPACE + "@onelake.dfs.fabric.microsoft.com";
    runtimeConf.put("trident.workspace.id", WORKSPACE);
    runtimeConf.put("trident.workspace.name", "sales-analytics");
    runtimeConf.put("trident.lakehouse.id", lakehouse);
    runtimeConf.put("trident.lakehouse.name", "lh_sales");
    OpenLineageContext fabric =
        OpenLineageContext.builder()
            .sparkSession(context.getSparkSession().get())
            .sparkContext(context.getSparkContext().get())
            .openLineage(context.getOpenLineage())
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
            .vendors(Vendors.getVendors())
            .build();

    OpenLineage.OutputDataset output =
        DatasetFactory.output(fabric)
            .sparkDatasetBuilder()
            .dataset(lakehouse + "/Tables/sales/orders", oneLake)
            .build();

    assertThat(output.getNamespace()).isEqualTo("fabric-lakehouse://" + WORKSPACE);
    assertThat(output.getName()).isEqualTo("lh_sales.sales.orders");
    OpenLineage.FabricOneLakeDatasetFacet facet = output.getFacets().getFabric_onelake();
    assertThat(facet.getItemType()).isEqualTo("Lakehouse");
    assertThat(facet.getItemName()).isEqualTo("lh_sales");
    assertThat(facet.getItemId()).isEqualTo(lakehouse);
    assertThat(facet.getWorkspaceName()).isEqualTo("sales-analytics");
    assertThat(facet.getLocation()).isEqualTo(oneLake + "/" + lakehouse + "/Tables/sales/orders");
    // outside Fabric, the vendor names nothing
    assertThat(
            DatasetIdentifierResolver.resolve(
                context, new DatasetIdentifier(lakehouse + "/Tables/sales/orders", oneLake)))
        .extracting(DatasetIdentifier::getNamespace)
        .isEqualTo(oneLake);
  }

  @Test
  void testWorkspaceNameOnlyForTheSessionsWorkspace() {
    runtimeConf.put("trident.workspace.id", WORKSPACE);
    hadoopConf.set("trident.workspace.name", "sales-analytics");

    assertThat(facet(NAMESPACE).get().getWorkspaceName()).isEqualTo("sales-analytics");

    // a table in another workspace, reached through that workspace's address
    String otherNamespace =
        "fabric-warehouse://"
            + FabricWarehouseUtils.endpointFor(
                UUID.fromString("68a4b712-9eec-4ca0-a2d3-6a96d2cf4f9c"),
                UUID.fromString(OTHER_WORKSPACE));
    OpenLineage.FabricOneLakeDatasetFacet other = facet(otherNamespace).get();
    assertThat(other.getWorkspaceId()).isEqualTo(OTHER_WORKSPACE);
    assertThat(other.getWorkspaceName()).isNull();
  }

  @Test
  void testNoFacetForOtherDatasets() {
    assertThat(facet("sqlserver://localhost:1433")).isEmpty();
    assertThat(
            DatasetFactory.input(context)
                .sparkDatasetBuilder()
                .dataset("raw/orders", "s3://bucket")
                .build()
                .getFacets()
                .getFabric_onelake())
        .isNull();
  }

  @Test
  void testFacetCanBeDisabled() {
    config.getFacetsConfig().getDisabledFacets().put("fabric_onelake", true);

    assertThat(facet(NAMESPACE)).isEmpty();
  }

  @Test
  void testWithWorkspaceNameKeepsKnownName() {
    OneLakeLocation location =
        OneLakeLocation.builder()
            .workspaceId(UUID.fromString(WORKSPACE))
            .workspaceName("from-path")
            .build();

    assertThat(
            OneLakeFacetUtils.withWorkspaceName(
                location,
                key ->
                    Optional.of("trident.workspace.id".equals(key) ? WORKSPACE : "from-session")))
        .isSameAs(location);
  }

  private Optional<OpenLineage.FabricOneLakeDatasetFacet> facet(String namespace) {
    return OneLakeFacetUtils.facetFor(
        context,
        new DatasetIdentifier("wh_primary.raw.orders", namespace, Collections.emptyList()));
  }
}
