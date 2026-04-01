/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.utils.TagField;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OpenLineageClientEnrichmentTest {

  static final URI PRODUCER = URI.create("https://test.producer");

  OpenLineage ol = new OpenLineage(PRODUCER);

  private OpenLineageClient clientWithConfig(OpenLineageConfig config) {
    return new OpenLineageClient(
        new ConsoleTransport(), null, new SimpleMeterRegistry(), config);
  }

  private OpenLineageConfig configWithJobTags(List<TagField> tags) {
    OpenLineageConfig config = new OpenLineageConfig();
    JobConfig jobConfig = new JobConfig();
    jobConfig.setTags(tags);
    config.setJobConfig(jobConfig);
    return config;
  }

  private OpenLineageConfig configWithRunTags(List<TagField> tags) {
    OpenLineageConfig config = new OpenLineageConfig();
    RunConfig runConfig = new RunConfig();
    runConfig.setTags(tags);
    config.setRunConfig(runConfig);
    return config;
  }

  // ---- Default version tag ----

  @Test
  void testDefaultVersionTagAddedToRunEvent() {
    OpenLineageClient client = clientWithConfig(new OpenLineageConfig());

    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    List<OpenLineage.TagsRunFacetFields> tags = enriched.getRun().getFacets().getTags().getTags();
    assertThat(tags)
        .anyMatch(
            t ->
                "openlineage_client_version".equals(t.getKey())
                    && "OPENLINEAGE_CLIENT".equals(t.getSource()));
  }

  @Test
  void testDefaultVersionTagNotAddedWhenNoConfig() {
    OpenLineageClient client = new OpenLineageClient(new ConsoleTransport());

    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    // no-op when config is null — event returned unchanged
    assertThat(enriched.getRun().getFacets().getTags()).isNull();
  }

  // ---- Job tags ----

  @Test
  void testJobTagsAddedFromConfig() {
    OpenLineageConfig config =
        configWithJobTags(Arrays.asList(new TagField("env", "production"), new TagField("team", "data")));
    OpenLineageClient client = clientWithConfig(config);

    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    List<OpenLineage.TagsJobFacetFields> tags = enriched.getJob().getFacets().getTags().getTags();
    assertThat(tags).anyMatch(t -> "env".equals(t.getKey()) && "production".equals(t.getValue()));
    assertThat(tags).anyMatch(t -> "team".equals(t.getKey()) && "data".equals(t.getValue()));
  }

  @Test
  void testJobTagsDefaultSourceIsConfig() {
    OpenLineageConfig config =
        configWithJobTags(Collections.singletonList(new TagField("env", "prod")));
    OpenLineageClient client = clientWithConfig(config);

    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    OpenLineage.TagsJobFacetFields tag =
        enriched.getJob().getFacets().getTags().getTags().stream()
            .filter(t -> "env".equals(t.getKey()))
            .findFirst()
            .get();
    assertThat(tag.getSource()).isEqualTo("CONFIG");
  }

  @Test
  void testJobTagsMergeWithExistingFacet() {
    OpenLineageConfig config =
        configWithJobTags(Collections.singletonList(new TagField("env", "staging")));
    OpenLineageClient client = clientWithConfig(config);

    // Pre-populate event with an existing tag
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.START)
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(ol.newRunFacetsBuilder().build()).build())
            .job(
                ol.newJobBuilder()
                    .namespace("ns")
                    .name("job")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .tags(
                                ol.newTagsJobFacetBuilder()
                                    .tags(
                                        Collections.singletonList(
                                            ol.newTagsJobFacetFields("existing_tag", "value", "INTEGRATION")))
                                    .build())
                            .build())
                    .build())
            .build();

    OpenLineage.RunEvent enriched = client.enrichRunEvent(event);

    List<OpenLineage.TagsJobFacetFields> tags = enriched.getJob().getFacets().getTags().getTags();
    // Both existing and config tags should be present
    assertThat(tags).anyMatch(t -> "existing_tag".equals(t.getKey()));
    assertThat(tags).anyMatch(t -> "env".equals(t.getKey()) && "staging".equals(t.getValue()));
  }

  @Test
  void testConfigTagOverridesExistingTagByKey() {
    OpenLineageConfig config =
        configWithJobTags(Collections.singletonList(new TagField("env", "production")));
    OpenLineageClient client = clientWithConfig(config);

    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.START)
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(ol.newRunFacetsBuilder().build()).build())
            .job(
                ol.newJobBuilder()
                    .namespace("ns")
                    .name("job")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .tags(
                                ol.newTagsJobFacetBuilder()
                                    .tags(
                                        Collections.singletonList(
                                            ol.newTagsJobFacetFields("env", "staging", "INTEGRATION")))
                                    .build())
                            .build())
                    .build())
            .build();

    OpenLineage.RunEvent enriched = client.enrichRunEvent(event);

    List<OpenLineage.TagsJobFacetFields> tags = enriched.getJob().getFacets().getTags().getTags();
    // Config tag should override the existing one
    assertThat(tags).hasSize(1);
    assertThat(tags.get(0).getValue()).isEqualTo("production");
  }

  // ---- Run tags ----

  @Test
  void testRunTagsAddedFromConfig() {
    OpenLineageConfig config =
        configWithRunTags(Collections.singletonList(new TagField("pipeline", "etl")));
    OpenLineageClient client = clientWithConfig(config);

    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    List<OpenLineage.TagsRunFacetFields> tags = enriched.getRun().getFacets().getTags().getTags();
    assertThat(tags).anyMatch(t -> "pipeline".equals(t.getKey()) && "etl".equals(t.getValue()));
  }

  // ---- Ownership ----

  @Test
  void testOwnershipAddedFromConfig() {
    OpenLineageConfig config = new OpenLineageConfig();
    JobConfig jobConfig = new JobConfig();
    JobConfig.JobOwnersConfig ownersConfig = new JobConfig.JobOwnersConfig();
    ownersConfig.getAdditionalProperties().put("team", "DataTeam");
    jobConfig.setOwnership(ownersConfig);
    config.setJobConfig(jobConfig);

    OpenLineageClient client = clientWithConfig(config);
    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    assertThat(enriched.getJob().getFacets().getOwnership()).isNotNull();
    assertThat(enriched.getJob().getFacets().getOwnership().getOwners())
        .anyMatch(o -> "team".equals(o.getType()) && "DataTeam".equals(o.getName()));
  }

  @Test
  void testOwnershipLegacyOwnersKeySupported() {
    OpenLineageConfig config = new OpenLineageConfig();
    JobConfig jobConfig = new JobConfig();
    JobConfig.JobOwnersConfig ownersConfig = new JobConfig.JobOwnersConfig();
    ownersConfig.getAdditionalProperties().put("team", "LegacyTeam");
    jobConfig.setOwners(ownersConfig); // legacy key
    config.setJobConfig(jobConfig);

    OpenLineageClient client = clientWithConfig(config);
    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    assertThat(enriched.getJob().getFacets().getOwnership()).isNotNull();
    assertThat(enriched.getJob().getFacets().getOwnership().getOwners())
        .anyMatch(o -> "team".equals(o.getType()) && "LegacyTeam".equals(o.getName()));
  }

  @Test
  void testOwnershipKeyTakesPrecedenceOverOwnersKey() {
    OpenLineageConfig config = new OpenLineageConfig();
    JobConfig jobConfig = new JobConfig();

    JobConfig.JobOwnersConfig legacyOwners = new JobConfig.JobOwnersConfig();
    legacyOwners.getAdditionalProperties().put("team", "LegacyTeam");
    jobConfig.setOwners(legacyOwners);

    JobConfig.JobOwnersConfig newOwnership = new JobConfig.JobOwnersConfig();
    newOwnership.getAdditionalProperties().put("team", "NewTeam");
    jobConfig.setOwnership(newOwnership);

    config.setJobConfig(jobConfig);

    OpenLineageClient client = clientWithConfig(config);
    OpenLineage.RunEvent enriched = client.enrichRunEvent(buildRunEvent());

    assertThat(enriched.getJob().getFacets().getOwnership().getOwners())
        .anyMatch(o -> "team".equals(o.getType()) && "NewTeam".equals(o.getName()));
  }

  @Test
  void testExistingOwnershipNotOverridden() {
    OpenLineageConfig config = new OpenLineageConfig();
    JobConfig jobConfig = new JobConfig();
    JobConfig.JobOwnersConfig ownersConfig = new JobConfig.JobOwnersConfig();
    ownersConfig.getAdditionalProperties().put("team", "ConfigTeam");
    jobConfig.setOwnership(ownersConfig);
    config.setJobConfig(jobConfig);

    OpenLineageClient client = clientWithConfig(config);

    // Pre-populate ownership in the event
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.START)
            .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(ol.newRunFacetsBuilder().build()).build())
            .job(
                ol.newJobBuilder()
                    .namespace("ns")
                    .name("job")
                    .facets(
                        ol.newJobFacetsBuilder()
                            .ownership(
                                ol.newOwnershipJobFacetBuilder()
                                    .owners(
                                        Collections.singletonList(
                                            ol.newOwnershipJobFacetOwnersBuilder()
                                                .name("IntegrationTeam")
                                                .type("team")
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();

    OpenLineage.RunEvent enriched = client.enrichRunEvent(event);

    // Existing ownership should NOT be overridden
    assertThat(enriched.getJob().getFacets().getOwnership().getOwners())
        .anyMatch(o -> "IntegrationTeam".equals(o.getName()));
  }

  // ---- Config loading ----

  @Test
  void testOwnershipConfigLoadedFromYaml() {
    final OpenLineageConfig config =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ConfigTest.TestConfigPathProvider("config/ownership.yaml"),
            new TypeReference<OpenLineageConfig>() {});

    assertThat(config.getJobConfig().getEffectiveOwners()).isNotNull();
    assertThat(config.getJobConfig().getEffectiveOwners().getAdditionalProperties())
        .containsEntry("team", "MyTeam")
        .containsEntry("person", "John Smith");
    assertThat(config.getJobConfig().getTags())
        .contains(new TagField("env", "production"));
    assertThat(config.getRunConfig().getTags())
        .contains(new TagField("pipeline", "etl"));
  }

  @Test
  void testLegacyOwnersConfigLoadedFromYaml() {
    final OpenLineageConfig config =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ConfigTest.TestConfigPathProvider("config/ownership-legacy.yaml"),
            new TypeReference<OpenLineageConfig>() {});

    assertThat(config.getJobConfig().getEffectiveOwners()).isNotNull();
    assertThat(config.getJobConfig().getEffectiveOwners().getAdditionalProperties())
        .containsEntry("team", "LegacyTeam");
  }

  // ---- JobEvent enrichment ----

  @Test
  void testJobEventEnrichedWithJobTags() {
    OpenLineageConfig config =
        configWithJobTags(Collections.singletonList(new TagField("env", "prod")));
    OpenLineageClient client = clientWithConfig(config);

    OpenLineage.JobEvent enriched = client.enrichJobEvent(buildJobEvent());

    assertThat(enriched.getJob().getFacets().getTags()).isNotNull();
    assertThat(enriched.getJob().getFacets().getTags().getTags())
        .anyMatch(t -> "env".equals(t.getKey()));
  }

  // ---- Helpers ----

  private OpenLineage.RunEvent buildRunEvent() {
    return ol.newRunEventBuilder()
        .eventType(OpenLineage.RunEvent.EventType.START)
        .run(ol.newRunBuilder().runId(UUID.randomUUID()).facets(ol.newRunFacetsBuilder().build()).build())
        .job(ol.newJobBuilder().namespace("ns").name("job").facets(ol.newJobFacetsBuilder().build()).build())
        .build();
  }

  private OpenLineage.JobEvent buildJobEvent() {
    return ol.newJobEventBuilder()
        .job(ol.newJobBuilder().namespace("ns").name("job").facets(ol.newJobFacetsBuilder().build()).build())
        .build();
  }
}
