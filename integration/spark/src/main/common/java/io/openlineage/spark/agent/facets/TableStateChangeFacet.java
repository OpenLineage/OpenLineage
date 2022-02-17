/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Facet used to notify state change performed on table like "CREATE", "DROP" or "TRUNCATE". */
@Getter
@EqualsAndHashCode(callSuper = false)
public class TableStateChangeFacet extends OpenLineage.DefaultDatasetFacet {

  public enum StateChange {
    CREATE,
    DROP,
    TRUNCATE,
    OVERWRITE;

    @JsonValue
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  @JsonProperty("stateChange")
  private StateChange stateChange;

  public TableStateChangeFacet(StateChange stateChange) {
    super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
    this.stateChange = stateChange;
  }
}
