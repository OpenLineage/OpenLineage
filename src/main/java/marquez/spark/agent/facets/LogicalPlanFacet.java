package marquez.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import java.net.URI;
import java.util.Map;
import lombok.Builder;
import marquez.spark.agent.client.LineageEvent.BaseFacet;

public class LogicalPlanFacet extends BaseFacet {
  private final Map<String, Object> plan;

  @Builder
  public LogicalPlanFacet(Map<String, Object> plan) {
    super(
        URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"),
        URI.create(
            "https://github.com/MarquezProject/marquez/blob/main/experimental/integrations/"
                + "marquez-spark-agent/facets/spark-2.4/v1/logicalPlanFacet"));
    this.plan = plan;
  }

  @JsonAnyGetter
  public Map<String, Object> getPlan() {
    return plan;
  }
}
