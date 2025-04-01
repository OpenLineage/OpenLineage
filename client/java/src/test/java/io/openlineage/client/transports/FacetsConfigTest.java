/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class FacetsConfigTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void shouldParseFacetsDisableProperties() {
    // Ignore unexpected and not properly nested properties
    assertThat(FacetsConfig.asDisabledFacetProperties("test1", "disabled")).isEmpty();
    assertThat(FacetsConfig.asDisabledFacetProperties("disabled", true)).isEmpty();

    // parse the 'disabled' properties for facets and ignore the other fields
    assertThat(
            FacetsConfig.asDisabledFacetProperties(
                "facetA", ImmutableMap.of("disabled", true, "otherField", "is ignored")))
        .containsExactly(new FacetsConfig.DisabledFacetProperty("facetA", true));

    // parse "nested" facets
    assertThat(
            FacetsConfig.asDisabledFacetProperties(
                "subsystemName",
                ImmutableMap.of(
                    "facetB",
                    ImmutableMap.of("disabled", true, "thisFieldShouldAlsoBeIgnored", "yes it is"),
                    "facetC",
                    ImmutableMap.of("disabled", false))))
        .containsExactly(
            new FacetsConfig.DisabledFacetProperty("subsystemName.facetB", true),
            new FacetsConfig.DisabledFacetProperty("subsystemName.facetC", false));

    // accept String values
    assertThat(
            FacetsConfig.asDisabledFacetProperties("facetI", ImmutableMap.of("disabled", "false")))
        .containsExactly(new FacetsConfig.DisabledFacetProperty("facetI", false));

    // but ignore the typos (don't treat them as false)
    assertThat(
            FacetsConfig.asDisabledFacetProperties("facetJ", ImmutableMap.of("disabled", "TRUE")))
        .isEmpty();
  }

  @Test
  void shouldParseWithJackson() throws JsonProcessingException {
    String json =
        "{\n"
            + "  \"facetD\": {\n"
            + "    \"disabled\": true\n"
            + "  },\n"
            + "  \"subsystemA\": {\n"
            + "    \"facetE\": {\n"
            + "      \"disabled\": false\n"
            + "    },\n"
            + "    \"facetF\": {\n"
            + "      \"disabled\": true,\n"
            + "      \"someOtherProperty\": \"someValue\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"facetG\": {\n"
            + "    \"disabled\": \"true\"\n"
            + "  },\n"
            + "  \"facetH\": {\n"
            + "    \"disabled\": \"TRUE\"\n"
            + "  }\n"
            + "}";

    FacetsConfig facetsConfig = MAPPER.readValue(json, FacetsConfig.class);
    assertThat(facetsConfig.getDisabledFacets())
        .containsAllEntriesOf(
            ImmutableMap.of(
                "facetD",
                true,
                "subsystemA.facetE",
                false,
                "subsystemA.facetF",
                true,
                "facetG",
                true));

    assertThat(facetsConfig.isFacetEnabled("facetD")).isFalse();
    assertThat(facetsConfig.isFacetEnabled("subsystemA.facetE")).isTrue();
  }
}
