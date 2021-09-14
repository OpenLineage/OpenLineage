package io.openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class SerializeOpenLineageEventTest {

  @Test
  public void testSerializeRunEvent() throws IOException {
    ObjectMapper mapper = OpenLineageClient.createMapper();

    ZonedDateTime dateTime = ZonedDateTime.parse("2021-01-01T00:00:01.000000000+02:00[UTC]");
    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);

    UUID runId = UUID.fromString("5f24c93c-2ce9-49dc-82e7-95ab4915242f");
    OpenLineage.RunFacets runFacets =
        ol.newRunFacets(ol.newNominalTimeRunFacet(dateTime, dateTime), null);
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
        ol.newJobFacets(documentationJobFacet, sourceCodeLocationJobFacet, sqlJobFacet);
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
        ol.newRunEvent("START", dateTime, run, job, inputs, outputs);

    String serialized = mapper.writeValueAsString(runStateUpdate);

    Path expectedDataPath =
        Paths.get("src", "test", "resources", "test_data", "serde", "openlineage-event.json");
    String expectedJson = new String(Files.readAllBytes(expectedDataPath));

    assertEquals(expectedJson, serialized);
  }
}
