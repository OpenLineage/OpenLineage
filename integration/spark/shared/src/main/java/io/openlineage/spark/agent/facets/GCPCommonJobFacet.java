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

public class GCPCommonJobFacet implements OpenLineage.JobFacet {

  private final URI _producer;
  private final URI _schemaURL;
  private final Boolean _deleted;
  @JsonAnySetter private final Map<String, Object> additionalProperties;

  public GCPCommonJobFacet(Map<String, Object> properties) {
    this(properties, null);
  }

  public GCPCommonJobFacet(Map<String, Object> properties, Boolean _deleted) {
    this._producer = Versions.OPEN_LINEAGE_PRODUCER_URI;
    this._schemaURL =
        URI.create(
            "https://openlineage.io/spec/facets/1-0-0/GcpCommonJobFacet.json#/$defs/GcpCommonJobFacet");
    this._deleted = _deleted;
    this.additionalProperties = new LinkedHashMap<>(properties);
  }

  public URI get_producer() {
    return this._producer;
  }

  public URI get_schemaURL() {
    return this._schemaURL;
  }

  public Boolean get_deleted() {
    return this._deleted;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }
}
