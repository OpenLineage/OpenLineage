/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.ParentJobFacet;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParentJobFacetTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private final OpenLineage openLineage =
      new OpenLineage(URI.create("https://example.com/parent-job"));

  @BeforeEach
  void setUp() {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  @Test
  void jobFacetsRoundTripParentJobFacetWithRoot() throws JsonProcessingException {
    ParentJobFacet parent =
        openLineage
            .newParentJobFacetBuilder()
            .job(
                openLineage
                    .newParentJobFacetJobBuilder()
                    .namespace("airflow")
                    .name("my_dag.my_task")
                    .build())
            .root(
                openLineage
                    .newParentJobFacetRootBuilder()
                    .job(
                        openLineage
                            .newRootJobIdentifierBuilder()
                            .namespace("airflow")
                            .name("my_dag")
                            .build())
                    .build())
            .build();
    JobFacets jobFacets = openLineage.newJobFacetsBuilder().parent(parent).build();

    String json = mapper.writeValueAsString(jobFacets);
    JsonNode serialized = mapper.readTree(json).get("parent");

    assertThat(serialized.get("_schemaURL").asText())
        .isEqualTo(
            "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json#/$defs/ParentJobFacet");
    assertThat(serialized.get("job").get("namespace").asText()).isEqualTo("airflow");
    assertThat(serialized.get("job").get("name").asText()).isEqualTo("my_dag.my_task");
    assertThat(serialized.get("root").get("job").get("name").asText()).isEqualTo("my_dag");
    assertThat(serialized.has("run")).isFalse();

    JobFacets deserialized = mapper.readValue(json, JobFacets.class);
    assertThat(deserialized.getParent().getJob().getName()).isEqualTo("my_dag.my_task");
    assertThat(deserialized.getParent().getRoot().getJob().getNamespace()).isEqualTo("airflow");
  }

  @Test
  void parentJobFacetWithoutRootOmitsRoot() throws JsonProcessingException {
    ParentJobFacet parent =
        openLineage
            .newParentJobFacetBuilder()
            .job(openLineage.newParentJobFacetJobBuilder().namespace("ns").name("parent").build())
            .build();

    JsonNode serialized = mapper.readTree(mapper.writeValueAsString(parent));

    assertThat(serialized.has("root")).isFalse();
    assertThat(serialized.get("job").get("name").asText()).isEqualTo("parent");
  }
}
