/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.NonNull;

/** Utilities class for {@link OpenLineageClient}. */
public final class OpenLineageClientUtils {
  private OpenLineageClientUtils() {}

  private static final ObjectMapper MAPPER = newObjectMapper();

  private static final ObjectMapper YML = new ObjectMapper(new YAMLFactory());

  @JsonFilter("disabledFacets")
  public class DisabledFacetsMixin {}

  /** Returns a new {@link ObjectMapper} instance. */
  public static ObjectMapper newObjectMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }

  /**
   * Configure object mapper to exclude specified facets from being serialized
   *
   * @param disableFacets
   */
  public static void configureObjectMapper(String[] disableFacets) {
    if (disableFacets == null) {
      return;
    }
    SimpleFilterProvider simpleFilterProvider =
        new SimpleFilterProvider()
            .addFilter(
                "disabledFacets", SimpleBeanPropertyFilter.serializeAllExcept(disableFacets));
    MAPPER.setFilterProvider(simpleFilterProvider);
    MAPPER.addMixIn(Object.class, DisabledFacetsMixin.class);
  }

  /** Converts the provided {@code value} to a Json {@code string}. */
  public static String toJson(@NonNull final Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Converts the provided Json {@code string} to the specified {@code type}. */
  public static <T> T fromJson(@NonNull final String json, @NonNull final TypeReference<T> type) {
    try {
      return MAPPER.readValue(json, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
    return MAPPER.convertValue(fromValue, toValueType);
  }

  /**
   * Create a new instance of the facets container with all values merged from the original
   * facetsContainer and the given facets Map, with precedence given to the facets Map.
   */
  public static <T, F> T mergeFacets(Map<String, F> facetsMap, T facetsContainer, Class<T> klass) {
    if (facetsContainer == null) {
      return MAPPER.convertValue(facetsMap, klass);
    }

    Map<String, F> targetMap =
        MAPPER.convertValue(facetsContainer, new TypeReference<Map<String, F>>() {});
    targetMap.putAll(facetsMap);
    return MAPPER.convertValue(targetMap, klass);
  }

  /** Converts the provided {@code urlString} to an {@link URI} object. */
  public static URI toUri(@NonNull final String urlString) {
    try {
      final String urlStringWithNoTrailingSlash =
          (urlString.endsWith("/") ? urlString.substring(0, urlString.length() - 1) : urlString);
      return new URI(urlStringWithNoTrailingSlash);
    } catch (URISyntaxException e) {
      final OpenLineageClientException error =
          new OpenLineageClientException("Malformed URI: " + urlString);
      error.initCause(e);
      throw error;
    }
  }

  public static OpenLineageYaml loadOpenLineageYaml(ConfigPathProvider configPathProvider) {
    try {
      for (final Path path : configPathProvider.getPaths()) {
        if (Files.exists(path)) {
          return YML.readValue(path.toFile(), OpenLineageYaml.class);
        }
      }
      throw new IllegalArgumentException();
    } catch (IOException e) {
      throw new OpenLineageClientException(e);
    }
  }
}
