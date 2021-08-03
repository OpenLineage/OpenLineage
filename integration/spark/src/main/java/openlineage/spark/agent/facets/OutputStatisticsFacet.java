package openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import openlineage.spark.agent.client.OpenLineageClient;

/**
 * Facet with statistics for an output dataset, including the number of rows and the size of the
 * output in bytes.
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class OutputStatisticsFacet extends OpenLineage.CustomFacet {
  long rowCount;
  long size;

  @Builder
  public OutputStatisticsFacet(long rowCount, long size) {
    super(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI));
    this.rowCount = rowCount;
    this.size = size;
  }
}
