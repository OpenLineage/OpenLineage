/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.facets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.TagsRunFacet;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RunTagsFacetBuilderTest {

  @Test
  void testAddTags() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    FlinkOpenLineageConfig config = new FlinkOpenLineageConfig();
    RunConfig runConfig = new RunConfig();
    runConfig.setTags(Arrays.asList(new TagField("label"), new TagField("key", "value", "SOURCE")));
    config.setRunConfig(runConfig);
    OpenLineageContext context =
        OpenLineageContext.builder().openLineage(openLineage).config(config).build();

    TagsRunFacet tagsFacet =
        RunTagsFacetBuilder.addTags(context, openLineage.newRunFacetsBuilder()).build().getTags();

    assertThat(tagsFacet.getTags())
        .extracting("key", "value", "source")
        .containsExactly(tuple("label", "true", "CONFIG"), tuple("key", "value", "SOURCE"));
  }

  static Stream<RunConfig> emptyRunConfigs() {
    RunConfig nullTags = new RunConfig();
    nullTags.setTags(null);
    return Stream.of(null, new RunConfig(), nullTags);
  }

  @ParameterizedTest
  @MethodSource("emptyRunConfigs")
  void testAddTagsWithoutRunTags(RunConfig runConfig) {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    FlinkOpenLineageConfig config = new FlinkOpenLineageConfig();
    config.setRunConfig(runConfig);
    OpenLineageContext context =
        OpenLineageContext.builder().openLineage(openLineage).config(config).build();

    assertThat(
            RunTagsFacetBuilder.addTags(context, openLineage.newRunFacetsBuilder())
                .build()
                .getTags())
        .isNull();
  }
}
