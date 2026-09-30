/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.run.RunConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;

/** Adds configured run tags to Flink run facets. */
public final class RunTagsFacetBuilder {
  private RunTagsFacetBuilder() {}

  public static OpenLineage.RunFacetsBuilder addTags(
      OpenLineageContext context, OpenLineage.RunFacetsBuilder builder) {
    RunConfig runConfig = context.getConfig().getRunConfig();
    if (runConfig == null || runConfig.getTags() == null || runConfig.getTags().isEmpty()) {
      return builder;
    }

    List<OpenLineage.TagsRunFacetFields> tags =
        runConfig.getTags().stream()
            .map(tag -> toTagsRunFacetFields(context.getOpenLineage(), tag))
            .collect(Collectors.toList());
    return builder.tags(context.getOpenLineage().newTagsRunFacetBuilder().tags(tags).build());
  }

  private static OpenLineage.TagsRunFacetFields toTagsRunFacetFields(
      OpenLineage openLineage, TagField tag) {
    return openLineage.newTagsRunFacetFields(tag.getKey(), tag.getValue(), tag.getSource());
  }
}
