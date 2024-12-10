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
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class IcebergCommitReportOutputDatasetFacet implements OpenLineage.OutputDatasetFacet {

  @JsonAnyGetter private final URI _producer;
  @JsonAnyGetter private final URI _schemaURL;
  @JsonAnyGetter private final Long snapshotId;
  @JsonAnyGetter private final Long sequenceNumber;
  @JsonAnyGetter private final String operation;
  @JsonAnyGetter private final IcebergCommitMetrics commitMetrics;
  @JsonAnyGetter private final Map<String, String> metadata;
  @JsonAnySetter private final Map<String, Object> additionalProperties;

  public IcebergCommitReportOutputDatasetFacet(
      Long snapshotId,
      Long sequenceNumber,
      String operation,
      IcebergCommitMetrics commitMetrics,
      Map<String, String> metadata,
      Map<String, Object> properties) {
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.operation = operation;
    this.commitMetrics = commitMetrics;
    this.metadata = metadata;
    this._producer = Versions.OPEN_LINEAGE_PRODUCER_URI;
    this._schemaURL =
        URI.create(
            "https://openlineage.io/spec/facets/1-0-0/IcebergCommitReportInputDatasetFacet.json");
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
