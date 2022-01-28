package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.util.Map;

/**
 * Facet used to report environment specific properties. For example, reporting the name of the
 * cluster used, reporting certain environment variables, or resolving mount points.
 */
public class EnvironmentFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("environment-properties")
  private Map<String, Object> properties;

  public EnvironmentFacet(Map<String, Object> environmentDetails) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.properties = environmentDetails;
  }
}
