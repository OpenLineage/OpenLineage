/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.LineageDatasetEntry;
import io.openlineage.client.OpenLineage.LineageDatasetInput;
import io.openlineage.client.OpenLineage.LineageEntry;
import io.openlineage.client.OpenLineage.LineageFieldEntry;
import io.openlineage.client.OpenLineage.LineageInput;
import io.openlineage.client.OpenLineage.LineageJobEntry;
import io.openlineage.client.OpenLineage.LineageJobInput;
import io.openlineage.client.OpenLineage.LineageTransformation;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LineageFacetTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private final OpenLineage openLineage =
      new OpenLineage(URI.create("https://example.com/lineage"));

  @BeforeEach
  void setUp() {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  @Test
  void jobFacetRoundTripsDatasetAndJobVariants() throws JsonProcessingException {
    LineageTransformation identity =
        openLineage.newLineageTransformationBuilder().type("DIRECT").subtype("IDENTITY").build();
    LineageDatasetInput datasetInput =
        openLineage
            .newLineageDatasetInputBuilder()
            .namespace("postgresql://warehouse")
            .name("raw.orders")
            .type(LineageDatasetInput.Type.DATASET)
            .field("customer_id")
            .transformations(Collections.singletonList(identity))
            .build();
    LineageJobInput jobInput =
        openLineage
            .newLineageJobInputBuilder()
            .namespace("https://example.com/jobs")
            .name("enrich_orders")
            .type(LineageJobInput.Type.JOB)
            .build();
    LineageFieldEntry fieldEntry =
        openLineage
            .newLineageFieldEntryBuilder()
            .inputs(Collections.<LineageInput>singletonList(datasetInput))
            .build();
    LineageDatasetEntry datasetEntry =
        openLineage
            .newLineageDatasetEntryBuilder()
            .namespace("postgresql://warehouse")
            .name("analytics.orders")
            .type(LineageDatasetEntry.Type.DATASET)
            .inputs(Arrays.<LineageInput>asList(datasetInput, jobInput))
            .fields(
                openLineage
                    .newLineageDatasetEntryFieldsBuilder()
                    .put("customer_id", fieldEntry)
                    .build())
            .build();
    LineageJobEntry jobEntry =
        openLineage
            .newLineageJobEntryBuilder()
            .namespace("https://example.com/jobs")
            .name("publish_orders")
            .type(LineageJobEntry.Type.JOB)
            .runId(UUID.randomUUID())
            .inputs(Collections.<LineageInput>singletonList(datasetInput))
            .build();
    JobFacets facets =
        openLineage
            .newJobFacetsBuilder()
            .lineage(
                openLineage
                    .newLineageJobFacetBuilder()
                    .entries(Arrays.<LineageEntry>asList(datasetEntry, jobEntry))
                    .build())
            .build();

    JobFacets roundTripped = mapper.readValue(mapper.writeValueAsString(facets), JobFacets.class);

    assertThat(roundTripped.getLineage().get_schemaURL().toString())
        .endsWith("LineageFacet.json#/$defs/LineageJobFacet");
    assertThat(roundTripped.getLineage().getEntries().get(0))
        .isInstanceOf(LineageDatasetEntry.class);
    assertThat(roundTripped.getLineage().getEntries().get(1)).isInstanceOf(LineageJobEntry.class);
    LineageDatasetEntry roundTrippedDataset =
        (LineageDatasetEntry) roundTripped.getLineage().getEntries().get(0);
    assertThat(roundTrippedDataset.getInputs().get(0)).isInstanceOf(LineageDatasetInput.class);
    assertThat(roundTrippedDataset.getInputs().get(1)).isInstanceOf(LineageJobInput.class);
  }

  @Test
  void datasetFacetPreservesExplicitEmptyInputs() throws JsonProcessingException {
    DatasetFacets facets =
        openLineage
            .newDatasetFacetsBuilder()
            .lineage(
                openLineage
                    .newLineageDatasetFacetBuilder()
                    .inputs(Collections.emptyList())
                    .fields(
                        openLineage
                            .newLineageDatasetFacetFieldsBuilder()
                            .put(
                                "generated_at",
                                openLineage
                                    .newLineageFieldEntryBuilder()
                                    .inputs(
                                        Collections.<LineageInput>singletonList(
                                            openLineage
                                                .newLineageJobInputBuilder()
                                                .type(LineageJobInput.Type.JOB)
                                                .transformations(
                                                    Collections.singletonList(
                                                        openLineage
                                                            .newLineageTransformationBuilder()
                                                            .type("DIRECT")
                                                            .subtype("GENERATION")
                                                            .build()))
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();

    String json = mapper.writeValueAsString(facets);
    DatasetFacets roundTripped = mapper.readValue(json, DatasetFacets.class);

    assertThat(json).contains("\"inputs\":[]");
    assertThat(roundTripped.getLineage().get_schemaURL().toString())
        .endsWith("LineageFacet.json#/$defs/LineageDatasetFacet");
    assertThat(roundTripped.getLineage().getInputs()).isEmpty();
    assertThat(
            roundTripped
                .getLineage()
                .getFields()
                .getAdditionalProperties()
                .get("generated_at")
                .getInputs()
                .get(0))
        .isInstanceOf(LineageJobInput.class);
  }

  @Test
  void jobIdentityRequiresNamespaceAndNameTogether() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            openLineage
                .newLineageJobEntryBuilder()
                .namespace("https://example.com/jobs")
                .type(LineageJobEntry.Type.JOB)
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            openLineage
                .newLineageJobInputBuilder()
                .name("upstream_job")
                .type(LineageJobInput.Type.JOB)
                .build());
  }
}
