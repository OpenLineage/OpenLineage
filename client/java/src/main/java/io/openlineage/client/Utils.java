/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.client;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.NonNull;

/** Utilities class for {@link OpenLineageClient}. */
public final class Utils {
  private Utils() {}

  private static final ObjectMapper MAPPER = newObjectMapper();

  private static final ObjectMapper YML = new ObjectMapper(new YAMLFactory());

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
