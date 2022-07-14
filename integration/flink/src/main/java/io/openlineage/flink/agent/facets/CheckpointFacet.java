package io.openlineage.flink.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.flink.agent.client.EventEmitter;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
/** Custom facet to contain counters from Flink checkpoints. */
public class CheckpointFacet extends OpenLineage.DefaultRunFacet {

  @JsonProperty("completed")
  private final int completed;

  @JsonProperty("failed")
  private final int failed;

  @JsonProperty("in-progress")
  private final int in_progress;

  @JsonProperty("restored")
  private final int restored;

  @JsonProperty("total")
  private final int total;

  public CheckpointFacet(int completed, int failed, int in_progress, int restored, int total) {
    super(EventEmitter.OPEN_LINEAGE_CLIENT_URI);
    this.completed = completed;
    this.failed = failed;
    this.in_progress = in_progress;
    this.restored = restored;
    this.total = total;
  }
}
