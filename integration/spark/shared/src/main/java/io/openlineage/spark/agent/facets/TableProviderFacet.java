/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.spark.agent.Versions;
import lombok.Getter;
import lombok.NonNull;

/**
 * Custom facet to provide extra information on table provider including properties: provider (e.g.
 * iceberg, delta-lake, etc.) and format (e.g. parquet, orc, etc.)
 */
@Getter
public class TableProviderFacet extends OpenLineage.DefaultDatasetFacet {

  @JsonProperty("provider")
  private String provider;

  @JsonProperty("format")
  private String format;

  public TableProviderFacet(@NonNull String provider, @NonNull String format) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.provider = provider;
    this.format = format;
  }
}
