package io.openlineage.spark3.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

public class DatasetVersionOutputDatasetFacetBuilderTest {
  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(Optional.of(mock(SparkSession.class)))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
          .queryExecution(queryExecution)
          .build();

  DataSourceV2Relation table = mock(DataSourceV2Relation.class, RETURNS_DEEP_STUBS);
  AppendData plan = mock(AppendData.class);
  Identifier identifier = Identifier.of(new String[] {}, "table");

  @BeforeEach
  public void setUp() {
    when(plan.table()).thenReturn(table);
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    when(table.identifier()).thenReturn(Option.apply(identifier));
    when(table.table().properties()).thenReturn(Collections.emptyMap());
  }

  @Test
  void testExtractIcebergDatasetVersionFacet() throws NoSuchTableException {
    TableCatalog catalog = mock(SparkCatalog.class);
    SparkTable sparkTable = mock(SparkTable.class, RETURNS_DEEP_STUBS);

    when(table.catalog()).thenReturn(Option.apply(catalog));

    when(catalog.loadTable(identifier)).thenReturn(sparkTable);
    when(sparkTable.table()).thenReturn(mock(org.apache.iceberg.Table.class));
    when(sparkTable.table().currentSnapshot()).thenReturn(mock(org.apache.iceberg.Snapshot.class));
    when(sparkTable.table().currentSnapshot().snapshotId()).thenReturn(1500100900L);
    checkBuilder();
  }

  @Test
  void testExtractDeltaDatasetVersionFacet() {
    DeltaCatalog catalog = mock(DeltaCatalog.class);
    DeltaTableV2 deltaTable = mock(DeltaTableV2.class, RETURNS_DEEP_STUBS);

    when(table.catalog()).thenReturn(Option.apply(catalog));
    when(catalog.loadTable(identifier)).thenReturn(deltaTable);
    when(deltaTable.snapshot().version()).thenReturn(1500100900L);
    checkBuilder();
  }

  void checkBuilder() {
    DatasetVersionOutputDatasetFacetBuilder builder =
        new DatasetVersionOutputDatasetFacetBuilder(openLineageContext);

    List<OpenLineage.DatasetFacet> facets = new ArrayList<>();
    builder.build(SparkListenerSQLExecutionEnd.apply(1L, 1L), (s, v) -> facets.add(v));

    assertThat(facets).hasSize(1);
    assertThat(facets.get(0))
        .isInstanceOfSatisfying(
            OpenLineage.DatasetVersionDatasetFacet.class,
            facet -> assertThat(facet.getDatasetVersion()).isEqualTo("1500100900"));
  }
}
