/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.lifecycle.MatchesMapRecursively;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OpenLineageRunEventTest {

  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};

  @Test
  public void testSerializeRunEvent() throws IOException, URISyntaxException {
    ObjectMapper mapper = OpenLineageClientUtils.newObjectMapper();

    ZonedDateTime dateTime = ZonedDateTime.parse("2021-01-01T00:00:01.000000000+00:00[UTC]");
    OpenLineage ol =
        new OpenLineage(
            new URI(
                "https://github.com/OpenLineage/OpenLineage/tree/0.2.3-SNAPSHOT/integration/spark"));

    UUID runId = UUID.fromString("5f24c93c-2ce9-49dc-82e7-95ab4915242f");
    OpenLineage.RunFacets runFacets =
        ol.newRunFacetsBuilder()
            .parent(
                ol.newParentRunFacet(
                    ol.newParentRunFacetRun(runId),
                    ol.newParentRunFacetJob("namespace", "jobName")))
            .errorMessage(ol.newErrorMessageRunFacet("failed", "JAVA", "<stack_trace>"))
            .build();
    OpenLineage.Run run = ol.newRun(runId, runFacets);
    OpenLineage.DocumentationJobFacet documentationJobFacet =
        ol.newDocumentationJobFacetBuilder().description("test documentation").build();
    OpenLineage.SourceCodeLocationJobFacet sourceCodeLocationJobFacet =
        ol.newSourceCodeLocationJobFacetBuilder()
            .branch("branch")
            .path("/path/to/file")
            .repoUrl("https://github.com/apache/spark")
            .type("git")
            .version("v1")
            .url(URI.create("https://github.com/apache/spark"))
            .tag("v1.0.0")
            .build();
    OpenLineage.SQLJobFacet sqlJobFacet = ol.newSQLJobFacet("SELECT * FROM test");

    OpenLineage.JobFacets jobFacets =
        ol.newJobFacetsBuilder()
            .sourceCodeLocation(sourceCodeLocationJobFacet)
            .sql(sqlJobFacet)
            .documentation(documentationJobFacet)
            .build();
    OpenLineage.Job job = ol.newJob("namespace", "jobName", jobFacets);
    List<OpenLineage.InputDataset> inputs =
        Arrays.asList(
            ol.newInputDataset(
                "ins",
                "input",
                null,
                ol.newInputDatasetInputFacetsBuilder()
                    .dataQualityMetrics(
                        ol.newDataQualityMetricsInputDatasetFacetBuilder()
                            .rowCount(10L)
                            .bytes(20L)
                            .columnMetrics(
                                ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
                                    .put(
                                        "mycol",
                                        ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder()
                                            .count(10D)
                                            .distinctCount(10L)
                                            .max(30D)
                                            .min(5D)
                                            .nullCount(1L)
                                            .sum(3000D)
                                            .quantiles(
                                                ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder()
                                                    .put("25", 52D)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build()));
    List<OpenLineage.OutputDataset> outputs =
        Arrays.asList(
            ol.newOutputDataset(
                "ons",
                "output",
                null,
                ol.newOutputDatasetOutputFacetsBuilder()
                    .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(10L, 20L))
                    .build()));
    OpenLineage.RunEvent runStateUpdate =
        ol.newRunEvent(OpenLineage.RunEvent.EventType.START, dateTime, run, job, inputs, outputs);

    Map<String, Object> actualJson =
        mapper.readValue(mapper.writeValueAsString(runStateUpdate), mapTypeReference);

    Path expectedDataPath =
        Paths.get("src", "test", "resources", "test_data", "serde", "openlineage-event.json");
    Map<String, Object> expectedJson =
        mapper.readValue(expectedDataPath.toFile(), mapTypeReference);

    assertThat(actualJson).satisfies(new MatchesMapRecursively(expectedJson));
  }
}
