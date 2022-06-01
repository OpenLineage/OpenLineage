/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.spark.shared.agent.Versions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.spark.SparkContext;

@Getter
public class SparkVersionFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("spark-version")
  private String sparkVersion;

  @JsonProperty("openlineage-spark-version")
  private String openlineageSparkVersion;

  public SparkVersionFacet(@NonNull SparkContext sparkContext) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.sparkVersion = sparkContext.version();
    this.openlineageSparkVersion = this.getClass().getPackage().getImplementationVersion();
  }
}
