/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonRawValue;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import lombok.Builder;
import lombok.ToString;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@ToString
public class LogicalPlanFacet extends OpenLineage.DefaultRunFacet {
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
