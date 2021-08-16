package openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonRawValue;
import io.openlineage.client.OpenLineage;
import lombok.Builder;
import lombok.ToString;
import openlineage.spark.agent.client.OpenLineageClient;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@ToString
public class LogicalPlanFacet extends OpenLineage.CustomFacet {
  private final LogicalPlan plan;

  @Builder
  public LogicalPlanFacet(LogicalPlan plan) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.plan = plan;
  }

  @JsonRawValue
  public String getPlan() {
    return plan.toJSON();
  }
}
