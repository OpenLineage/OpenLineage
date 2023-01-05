/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link OpenLineageClientUtils}. */
public class OpenLineageClientUtilsTest {
  private static final String VALUE = "test";
  private static final Object OBJECT = new Object(VALUE);
  private static final TypeReference<Object> TYPE = new TypeReference<Object>() {};
  private static final String JSON = "{\"value\":\"" + VALUE + "\"}";

  @BeforeEach
  public void setUp() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {});
  }

  @Test
  public void testToJson() {
    final String actual = OpenLineageClientUtils.toJson(OBJECT);
    assertThat(actual).isEqualTo(JSON);
  }

  @Test
  public void testToJson_withDisabledFacets() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {"excludedValue"});
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).doesNotContain("excludedValue");
  }

  @Test
  public void testToJson_withDisabledFacetsIsNull() {
    OpenLineageClientUtils.configureObjectMapper(null);
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).contains("excludedValue");
  }

  @Test
  public void testToJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.toJson(null));
  }

  @Test
  public void testFromJson() {
    final Object actual = OpenLineageClientUtils.fromJson(JSON, TYPE);
    assertThat(actual).isEqualToComparingFieldByField(OBJECT);
  }

  @Test
  public void testFromJson_throwsOnNull() {
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
  public void testToUrl_throwsOnNull() {
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
}
