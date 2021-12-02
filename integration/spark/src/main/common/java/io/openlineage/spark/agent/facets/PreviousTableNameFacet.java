package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import lombok.Getter;

/**
 * Facet used to indicate previous table name, when table was renamed as a result of a spark job
 * execution
 */
@Getter
public class PreviousTableNameFacet extends OpenLineage.DefaultDatasetFacet {

  @JsonProperty("previousTableURI")
  private String previousTableUri;

  @JsonProperty("currentTableURI")
  private String currentTableUri;

  /**
   * @param previousTableUri previous table name
   * @param currentTableUri current table name
   */
  public PreviousTableNameFacet(String previousTableUri, String currentTableUri) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.previousTableUri = previousTableUri;
    this.currentTableUri = currentTableUri;
  }
}
