package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.net.URI;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

/**
 * Facet with statistics for an output dataset, including the number of rows and the size of the
 * output in bytes.
 *
 * @deprecated in favor of the {@link
 *     io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet} in the OpenLineage core
 *     spec.
 */
@EqualsAndHashCode(callSuper = true)
@Value
@Deprecated
public class OutputStatisticsFacet extends OpenLineage.DefaultDatasetFacet {
  long rowCount;
  long size;
  String status = "DEPRECATED";

  @Builder
  public OutputStatisticsFacet(long rowCount, long size) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.rowCount = rowCount;
    this.size = size;
  }

  @Override
  public URI get_schemaURL() {
    return URI.create(
        OpenLineageClient.OPEN_LINEAGE_CLIENT_URI
            + "/facets/spark/v1/output-statistics-facet.json");
  }
}
