package marquez.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonRawValue;
import java.net.URI;
import lombok.Builder;
import lombok.ToString;
import marquez.spark.agent.client.LineageEvent.BaseFacet;
import marquez.spark.agent.client.OpenLineageClient;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@ToString
public class LogicalPlanFacet extends BaseFacet {
  private final LogicalPlan plan;

  @Builder
  public LogicalPlanFacet(LogicalPlan plan) {
    super(
        URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI),
        URI.create(
            OpenLineageClient.OPEN_LINEAGE_CLIENT_URI + "/facets/spark-2.4/v1/logicalPlanFacet"));
    this.plan = plan;
  }

  @JsonRawValue
  public String getPlan() {
    return plan.toJSON();
  }
}
