/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.shaded.com.fasterxml.jackson.annotation.JsonRawValue;
import io.openlineage.spark.shared.agent.Versions;
import lombok.Builder;
import lombok.ToString;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@ToString
public class LogicalPlanFacet extends OpenLineage.DefaultRunFacet {
  private final LogicalPlan plan;

  @Builder
  public LogicalPlanFacet(LogicalPlan plan) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.plan = plan;
  }

  @JsonRawValue
  public String getPlan() {
    return plan.toJSON();
  }
}
