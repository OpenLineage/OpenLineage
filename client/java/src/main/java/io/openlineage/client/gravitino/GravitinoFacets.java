/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.gravitino;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.openlineage.client.OpenLineage.DatasetFacet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

public class GravitinoFacets {

  private static final URI producer;

  static {
    try {
      producer = new URI("https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /** model class for LocationDatasetFacet */
  @JsonDeserialize(as = GravitinoFacets.LocationDatasetFacet.class)
  @JsonPropertyOrder({
    "_producer",
    "_schemaURL",
    "_deleted",
    "location",
  })
  public static final class LocationDatasetFacet implements DatasetFacet {

    private final URI _producer;

    private final URI _schemaURL;

    private final Boolean _deleted;

    private final String location;

    @JsonAnySetter private final Map<String, Object> additionalProperties;

    /**
     * @param _producer URI identifying the producer of this metadata. For example this could be a
     *     git url with a given tag or sha
     * @param location Storage layer provider with allowed values: iceberg, delta.
     */
    @JsonCreator
    private LocationDatasetFacet(
        @JsonProperty("_producer") URI _producer, @JsonProperty("location") String location) {
      this(
          _producer,
          URI.create(
              "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet"),
          location);
    }

    private LocationDatasetFacet(URI _producer, URI _schemaURL, String location) {
      this._producer = _producer;
      this._schemaURL = _schemaURL;
      this._deleted = Boolean.FALSE;
      this.location = location;
      this.additionalProperties = new LinkedHashMap<>();
    }

    /**
     * @return URI identifying the producer of this metadata. For example this could be a git url
     *     with a given tag or sha
     */
    @Override
    public URI get_producer() {
      return _producer;
    }

    /**
     * @return The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding
     *     version of the schema definition for this facet
     */
    @Override
    public URI get_schemaURL() {
      return _schemaURL;
    }

    /**
     * @return set to true to delete a facet
     */
    @Override
    public Boolean get_deleted() {
      return _deleted;
    }

    /**
     * @return the location.
     */
    public String getLocation() {
      return location;
    }

    /**
     * @return additional properties
     */
    @JsonAnyGetter
    @Override
    public Map<String, Object> getAdditionalProperties() {
      return additionalProperties;
    }

    /** Get object with additional properties */
    void withAdditionalProperties() {}
  }

  public static LocationDatasetFacet newLocationDatasetFact(String location) {
    return new LocationDatasetFacet(producer, location);
  }
}
