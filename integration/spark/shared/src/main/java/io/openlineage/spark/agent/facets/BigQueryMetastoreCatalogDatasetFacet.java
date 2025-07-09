/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class BigQueryMetastoreCatalogDatasetFacet implements OpenLineage.DatasetFacet {

  @JsonAnyGetter private final URI _producer;
  @JsonAnyGetter private final URI _schemaURL;
  @JsonAnyGetter private final Boolean _deleted;
  @JsonAnyGetter private final String framework;
  @JsonAnyGetter private final String type;
  @JsonAnyGetter private final String name;
  @JsonAnyGetter private final String metadataUri;
  @JsonAnyGetter private final String warehouseUri;
  @JsonAnyGetter private final String source;
  @JsonAnyGetter private final String projectId;
  @JsonAnyGetter private final String location;
  @JsonAnySetter private final Map<String, Object> additionalProperties;

  @JsonCreator
  public BigQueryMetastoreCatalogDatasetFacet(
      @JsonProperty("framework") String framework,
      @JsonProperty("type") String type,
      @JsonProperty("name") String name,
      @JsonProperty("metadataUri") String metadataUri,
      @JsonProperty("warehouseUri") String warehouseUri,
      @JsonProperty("source") String source,
      @JsonProperty("projectId") String projectId,
      @JsonProperty("location") String location) {
    this._producer = Versions.OPEN_LINEAGE_PRODUCER_URI;
    this._schemaURL =
        URI.create(
            "https://openlineage.io/spec/facets/1-0-0/BigQueryMetastoreCatalogDatasetFacet.json#/$defs/BigQueryMetastoreCatalogDatasetFacet");
    this._deleted = null;
    this.framework = framework;
    this.type = type;
    this.name = name;
    this.metadataUri = metadataUri;
    this.warehouseUri = warehouseUri;
    this.source = source;
    this.projectId = projectId;
    this.location = location;
    this.additionalProperties = new LinkedHashMap<>();
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
