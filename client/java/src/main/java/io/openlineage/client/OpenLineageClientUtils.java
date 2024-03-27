/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.openlineage.client.OpenLineage.RunEvent;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for {@link OpenLineageClient} that provides common functionalities for object
 * mapping, JSON and YAML parsing, and URI manipulation.
 */
@Slf4j
public final class OpenLineageClientUtils {
  private OpenLineageClientUtils() {}

  private static final ObjectMapper MAPPER = newObjectMapper();

  private static final ObjectMapper YML = newObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON = newObjectMapper();

  @JsonFilter("disabledFacets")
  public class DisabledFacetsMixin {}

  /**
   * Creates a new {@link ObjectMapper} instance configured with modules for JDK8 and JavaTime,
   * including settings to ignore unknown properties and to not write dates as timestamps.
   *
   * @return A configured {@link ObjectMapper} instance.
   */
  public static ObjectMapper newObjectMapper() {
    return newObjectMapper(new JsonFactory());
  }

  /**
   * Creates a new {@link ObjectMapper} instance configured with modules for JDK8 and JavaTime,
   * including settings to ignore unknown properties and to not write dates as timestamps.
   *
   * @return A configured {@link ObjectMapper} instance.
   */
  public static ObjectMapper newObjectMapper(JsonFactory jsonFactory) {
    final ObjectMapper mapper = new ObjectMapper(jsonFactory);
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }

  /**
   * Configures the object mapper to exclude specified facets from being serialized.
   *
   * @param disableFacets Array of facet names to be excluded from serialization.
   */
  public static void configureObjectMapper(String... disableFacets) {
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

  /**
   * Converts the provided value to a JSON string.
   *
   * @param value The object to be converted to JSON.
   * @return A JSON string representation of the object.
   * @throws UncheckedIOException If an I/O error occurs during conversion.
   */
  public static String toJson(@NonNull final Object value) throws UncheckedIOException {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Converts the provided JSON string to an instance of the specified type.
   *
   * @param json The JSON string to be converted.
   * @param type The type to convert the JSON string into.
   * @param <T> The generic type of the return value.
   * @return An instance of the specified type.
   * @throws UncheckedIOException If an I/O error occurs during conversion.
   */
  public static <T> T fromJson(@NonNull final String json, @NonNull final TypeReference<T> type)
      throws UncheckedIOException {
    try {
      return MAPPER.readValue(json, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Convenience method to convert a JSON string directly into a {@link RunEvent} instance.
   *
   * @param json The JSON string representing a {@link RunEvent}.
   * @return An instance of {@link RunEvent}.
   * @throws UncheckedIOException If an I/O error occurs during conversion.
   */
  public static RunEvent runEventFromJson(@NonNull final String json) throws UncheckedIOException {
    return fromJson(json, new TypeReference<RunEvent>() {});
  }

  /**
   * Converts the value of an object from one type to another.
   *
   * @param fromValue The object whose value is to be converted.
   * @param toValueType The target type for the conversion.
   * @param <T> The generic type of the target type.
   * @return An object of the target type with the value converted from the original object.
   */
  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {
    return MAPPER.convertValue(fromValue, toValueType);
  }

  /**
   * Merges the given facets map with an existing facets container, giving precedence to the values
   * in the facets map.
   *
   * @param facetsMap A map containing facets to be merged.
   * @param facetsContainer The existing container of facets.
   * @param klass The class of the facets container.
   * @param <T> The type of the facets container.
   * @param <F> The type of facets in the map.
   * @return A new instance of the facets container with merged values.
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

  /**
   * Converts a string URL into an {@link URI} object.
   *
   * @param urlString The string URL to be converted.
   * @return An {@link URI} object.
   * @throws OpenLineageClientException If the given string does not conform to the URI
   *     specification.
   */
  public static URI toUri(@NonNull final String urlString) throws OpenLineageClientException {
    try {
      final String urlStringWithNoTrailingSlash =
          (urlString.endsWith("/") ? urlString.substring(0, urlString.length() - 1) : urlString);
      return new URI(urlStringWithNoTrailingSlash);
    } catch (URISyntaxException e) {
      throw new OpenLineageClientException("Malformed URI: " + urlString, e);
    }
  }

  /**
   * Loads and parses OpenLineage configuration from the provided paths. Throws an {@link
   * OpenLineageClientException} if one of the following conditions are met:
   *
   * <ol>
   *   <li>The provided configPathProvider is null
   *   <li>No configuration file could be found at any of the provided paths
   *   <li>Load the default configuration from the classpath if no file is found
   * </ol>
   *
   * @param configPathProvider Provides the paths where the configuration files can be found.
   * @return An instance of {@link OpenLineageYaml} containing the parsed configuration.
   * @throws OpenLineageClientException According to the rules defined above.
   */
  public static OpenLineageYaml loadOpenLineageYaml(ConfigPathProvider configPathProvider)
      throws OpenLineageClientException {
    try {
      Objects.requireNonNull(configPathProvider);
      List<Path> paths = configPathProvider.getPaths();
      for (final Path path : paths) {
        if (Files.exists(path)) {
          return YML.readValue(path.toFile(), OpenLineageYaml.class);
        }
      }
      String concatenatedPaths =
          paths.stream().map(Path::toString).collect(Collectors.joining(";", "[", "]"));
      throw new FileNotFoundException(
          "No OpenLineage configuration file found at provided paths, looked in: "
              + concatenatedPaths);
    } catch (NullPointerException e) {
      throw new OpenLineageClientException("ConfigPathProvider was null");
    } catch (FileNotFoundException e) {
      throw new OpenLineageClientException("No OpenLineage configuration file found");
    } catch (IOException e) {
      throw new OpenLineageClientException(e);
    }
  }

  /**
   * Loads and parses OpenLineage YAML configuration from an {@link InputStream}.
   *
   * @param inputStream The {@link InputStream} from which to load the configuration.
   * @return An instance of {@link OpenLineageYaml} containing the parsed YAML configuration.
   * @throws OpenLineageClientException If an error occurs while reading or parsing the
   *     configuration.
   */
  public static OpenLineageYaml loadOpenLineageYaml(InputStream inputStream)
      throws OpenLineageClientException {
    try {
      return deserializeInputStream(YML, inputStream);
    } catch (IOException e) {
      log.warn("Error deserializing OpenLineage YAML, falling back to JSON", e);
      // Some clients may actually pass in a JSON, because of how this method used to operate
      // So, we'll use OpenLineageClientUtils#loadOpenLineageJson(InputStream) as a fallback
      return loadOpenLineageJson(inputStream);
    }
  }

  /**
   * Loads and parses OpenLineage JSON configuration from an {@link InputStream}.
   *
   * @param inputStream The {@link InputStream} from which to load the configuration.
   * @return An instance of {@link OpenLineageYaml} containing the parsed JSON configuration.
   * @throws OpenLineageClientException If an error occurs while reading or parsing the
   *     configuration.
   */
  public static OpenLineageYaml loadOpenLineageJson(InputStream inputStream)
      throws OpenLineageClientException {
    try {
      return deserializeInputStream(JSON, inputStream);
    } catch (IOException e) {
      throw new OpenLineageClientException(e);
    }
  }

  /**
   * Internal method to deserialize an {@link InputStream} into an {@link OpenLineageYaml} instance,
   * using the specified {@link ObjectMapper} for either JSON or YAML.
   *
   * @param deserializer The {@link ObjectMapper} to use for deserialization.
   * @param inputStream The {@link InputStream} containing the configuration data.
   * @return An instance of {@link OpenLineageYaml}.
   * @throws IOException If an error occurs while reading or parsing the configuration.
   */
  private static OpenLineageYaml deserializeInputStream(
      ObjectMapper deserializer, InputStream inputStream) throws IOException {
    return deserializer.readValue(inputStream, OpenLineageYaml.class);
  }
}
