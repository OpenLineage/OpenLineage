/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.TransportConfig;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link OpenLineageClientUtils}. */
class OpenLineageClientUtilsTest {
  private static final String VALUE = "test";
  private static final Object OBJECT = new Object(VALUE);
  private static final TypeReference<Object> TYPE = new TypeReference<Object>() {};
  private static final String JSON = "{\"value\":\"" + VALUE + "\"}";

  @BeforeEach
  public void setUp() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {});
  }

  @Test
  void testToJson() {
    final String actual = OpenLineageClientUtils.toJson(OBJECT);
    assertThat(actual).isEqualTo(JSON);
  }

  @Test
  void testToJson_withDisabledFacets() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {"excludedValue"});
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).doesNotContain("excludedValue");
  }

  @Test
  void testToJson_withDisabledFacetsIsNull() {
    OpenLineageClientUtils.configureObjectMapper();
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).contains("excludedValue");
  }

  @Test
  void testToJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.toJson(null));
  }

  @Test
  void testFromJson() {
    final Object actual = OpenLineageClientUtils.fromJson(JSON, TYPE);
    assertThat(actual).isEqualToComparingFieldByField(OBJECT);
  }

  @Test
  void testFromJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.fromJson(JSON, null));
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.fromJson(null, TYPE));
  }

  @Test
  void testToUrl() throws Exception {
    final String urlString = "http://test.com:8080";
    final URI expected = new URI(urlString);
    final URI actual = OpenLineageClientUtils.toUri(urlString);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testToUrl_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.toUri(null));
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  static final class Object {
    final String value;

    @JsonCreator
    Object(@JsonProperty("value") final String value) {
      this.value = value;
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  static final class ObjectWithDisabledFacets {
    final String excludedValue;
    final String notExcludedValue;

    @JsonCreator
    ObjectWithDisabledFacets(
        @JsonProperty("excludedValue") final String excludedValue,
        @JsonProperty("notExcludedValue") final String notExcludedValue) {
      this.excludedValue = excludedValue;
      this.notExcludedValue = notExcludedValue;
    }
  }

  @Test
  void loadOpenLineageYaml_shouldDeserialiseYamlEncodedInputStreams() {
    String yamlString =
        "transport:\n"
            + "  type: http\n"
            + "  url: http://localhost:5050\n"
            + "  endpoint: api/v1/lineage\n";
    System.out.println(yamlString);

    byte[] bytes = yamlString.getBytes(StandardCharsets.UTF_8);

    OpenLineageYaml yaml =
        OpenLineageClientUtils.loadOpenLineageYaml(new ByteArrayInputStream(bytes));
    TransportConfig transportConfig = yaml.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("http://localhost:5050"));
    assertThat(httpConfig.getEndpoint()).isEqualTo("api/v1/lineage");
  }

  @Test
  void loadOpenLineageYaml_shouldFallbackAndDeserialiseJsonEncodedInputStreams() {
    byte[] bytes =
        "{\"transport\":{\"type\":\"http\",\"url\":\"https://localhost:1234/api/v1/lineage\"}}"
            .getBytes(StandardCharsets.UTF_8);

    OpenLineageYaml yaml =
        OpenLineageClientUtils.loadOpenLineageYaml(new ByteArrayInputStream(bytes));
    TransportConfig transportConfig = yaml.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("https://localhost:1234/api/v1/lineage"));
  }

  @Test
  void loadOpenLineageJson_ShouldDeserialiseJsonEncodedInputStreams() {
    byte[] bytes =
        "{\"transport\":{\"type\":\"http\",\"url\":\"https://localhost:1234/api/v1/lineage\"}}"
            .getBytes(StandardCharsets.UTF_8);

    OpenLineageYaml yaml =
        OpenLineageClientUtils.loadOpenLineageJson(new ByteArrayInputStream(bytes));
    TransportConfig transportConfig = yaml.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("https://localhost:1234/api/v1/lineage"));
  }
}
