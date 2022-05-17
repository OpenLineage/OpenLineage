package io.openlineage.flink.tracker.restapi;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
/** Class representing Flink REST API for endpoint /jobs/:jobid/checkpoints */
public class Checkpoints {
  CheckpointsCounts counts;
}
