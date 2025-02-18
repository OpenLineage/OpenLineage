/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.TagField;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class TagsRunFacetBuilder extends CustomFacetBuilder<Object, OpenLineage.TagsRunFacet> {
  private OpenLineageContext context;

  public TagsRunFacetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  protected void build(
      Object event, BiConsumer<String, ? super OpenLineage.TagsRunFacet> consumer) {
    if (context.getOpenLineageConfig().getRunConfig() == null) {
      return;
    }
    List<TagField> tags = context.getOpenLineageConfig().getRunConfig().getTags();
    if (!tags.isEmpty()) {
      consumer.accept(
          "tags",
          this.context
              .getOpenLineage()
              .newTagsRunFacetBuilder()
              .tags(
                  tags.stream()
                      .map(
                          x ->
                              this.context
                                  .getOpenLineage()
                                  .newTagsRunFacetFields(x.getKey(), x.getValue(), x.getSource()))
                      .collect(Collectors.toList()))
              .build());
    }
  }
}
