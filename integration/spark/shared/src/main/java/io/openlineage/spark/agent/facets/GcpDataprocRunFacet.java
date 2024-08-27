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

public class GcpDataprocRunFacet implements OpenLineage.RunFacet {

  private final URI _producer;
  private final URI _schemaURL;
  @JsonAnySetter private final Map<String, Object> additionalProperties;

  public GcpDataprocRunFacet(Map<String, Object> properties) {
    this._producer = Versions.OPEN_LINEAGE_PRODUCER_URI;
    this._schemaURL =
        URI.create(
            "https://openlineage.io/spec/facets/1-0-0/GcpDataprocRunFacet.json#/$defs/GcpDataprocRunFacet");
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
