package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
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
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.sparkVersion = sparkContext.version();
    this.openlineageSparkVersion = this.getClass().getPackage().getImplementationVersion();
  }
}
