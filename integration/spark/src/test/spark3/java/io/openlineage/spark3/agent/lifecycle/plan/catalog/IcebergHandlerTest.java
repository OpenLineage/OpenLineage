package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(SparkAgentTestExtension.class)
public class IcebergHandlerTest {

  private IcebergHandler icebergHandler = new IcebergHandler();

  @ParameterizedTest
  @CsvSource({
    "hdfs://namenode:8020/warehouse,hdfs://namenode:8020,/warehouse/database.schema.table",
    "/tmp/warehouse,file,/tmp/warehouse/database.schema.table"
  })
  public void testGetDatasetIdentifierForHadoop(
      String warehouseConf, String namespace, String name) {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    session.conf().set("spark.sql.catalog.test.type", "hadoop");
    session.conf().set("spark.sql.catalog.test.warehouse", warehouseConf);

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            session,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals(name, datasetIdentifier.getName());
    assertEquals(namespace, datasetIdentifier.getNamespace());
  }

  @Test
  public void testGetDatasetIdentifierForHive() {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    session.conf().set("spark.sql.catalog.test.type", "hive");
    session.conf().set("spark.sql.catalog.test.uri", "thrift://metastore-host:10001");

    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    DatasetIdentifier datasetIdentifier =
        icebergHandler.getDatasetIdentifier(
            session,
            sparkCatalog,
            Identifier.of(new String[] {"database", "schema"}, "table"),
            new HashMap<>());

    assertEquals("database.schema.table", datasetIdentifier.getName());
    assertEquals("hive://metastore-host:10001", datasetIdentifier.getNamespace());
  }

  @Test
  public void testGetTableProviderFacet() {
    Optional<TableProviderFacet> tableProviderFacet =
        icebergHandler.getTableProviderFacet(Collections.singletonMap("format", "iceberg/parquet"));
    assertEquals("iceberg", tableProviderFacet.get().getProvider());
    assertEquals("parquet", tableProviderFacet.get().getFormat());
  }

  @Test
  public void testGetTableProviderFacetWhenFormatNotProvided() {
    Optional<TableProviderFacet> tableProviderFacet =
        icebergHandler.getTableProviderFacet(new HashMap<>());
    assertEquals("iceberg", tableProviderFacet.get().getProvider());
    assertEquals("", tableProviderFacet.get().getFormat());
  }

  @Test
  public void testGetVersionString() throws NoSuchTableException {
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);
    Identifier identifier = Identifier.of(new String[] {"database", "schema"}, "table");

    when(sparkCatalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table().currentSnapshot().snapshotId()).thenReturn(1500100900L);

    Optional<String> version =
        icebergHandler.getDatasetVersion(sparkCatalog, identifier, Collections.emptyMap());

    assertTrue(version.isPresent());
    assertEquals(version.get(), "1500100900");
  }
}
