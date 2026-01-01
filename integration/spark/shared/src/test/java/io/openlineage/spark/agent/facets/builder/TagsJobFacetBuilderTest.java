/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class TagsJobFacetBuilderTest {
  @Test
  void testTagsJobEventBuilds() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    JobConfig jobConfig = new JobConfig();
    jobConfig.setTags(
        Arrays.asList(
            new TagField("label"),
            new TagField("k", "v"),
            new TagField("k", "v", "s"),
            new TagField("e", "f", "g")));
    config.setJobConfig(jobConfig);
    when(context.getOpenLineageConfig()).thenReturn(config);
    when(context.getOpenLineage()).thenReturn(openLineage);

    TagsJobFacetBuilder builder = new TagsJobFacetBuilder(context);
    builder.accept(
        new Object(),
        (s, tagsJobFacet) ->
            assertThat(tagsJobFacet.getTags())
                .hasSize(4)
                .extracting("key", "value", "source")
                .containsExactly(
                    tuple("label", "true", "CONFIG"),
                    tuple("k", "v", "CONFIG"),
                    tuple("k", "v", "s"),
                    tuple("e", "f", "g")));
  }
}
