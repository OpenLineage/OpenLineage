package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Map;

public class SparkPropertyFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("properties")
  @SuppressWarnings("PMD")
  private Map<String, Object> properties;

  public Map<String, Object> getProperties() {
    return properties;
  }

  public SparkPropertyFacet(Map<String, Object> environmentDetails) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.properties = environmentDetails;
  }
}
