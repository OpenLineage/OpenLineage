/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class IcebergScanReportInputDatasetFacet implements OpenLineage.InputDatasetFacet {

  private final URI _producer;
  private final URI _schemaURL;
  @JsonAnySetter private final Long snapshotId;
  @JsonAnySetter private final String filterDescription;
  @JsonAnySetter private final String[] projectedFieldNames;
  @JsonAnySetter private final IcebergScanMetrics scanMetrics;
  @JsonAnyGetter private final Map<String, String> metadata;
  @JsonAnySetter private final Map<String, Object> additionalProperties;

  public IcebergScanReportInputDatasetFacet(
      Long snapshotId,
      String filterDescription,
      String[] projectedFieldNames,
      IcebergScanMetrics scanMetrics,
      Map<String, String> metadata,
      Map<String, Object> properties) {
    this.snapshotId = snapshotId;
    this.filterDescription = filterDescription;
    this.projectedFieldNames = Arrays.copyOf(projectedFieldNames, projectedFieldNames.length);
    this.scanMetrics = scanMetrics;
    this.metadata = metadata;
    this._producer = Versions.OPEN_LINEAGE_PRODUCER_URI;
    this._schemaURL =
        URI.create(
            "https://openlineage.io/spec/facets/1-0-0/IcebergScanReportInputDatasetFacet.json");
    this.additionalProperties = new LinkedHashMap<>(properties);
  }

  @Override
  public URI get_producer() {
    return this._producer;
  }

  @Override
  public URI get_schemaURL() {
    return this._schemaURL;
  }

  @JsonAnyGetter
  @Override
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }
}
