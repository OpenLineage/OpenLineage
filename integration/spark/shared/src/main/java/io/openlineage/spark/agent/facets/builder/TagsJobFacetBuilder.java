/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.TagField;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class TagsJobFacetBuilder extends CustomFacetBuilder<Object, OpenLineage.TagsJobFacet> {
  private OpenLineageContext context;

  public TagsJobFacetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  protected void build(
      Object event, BiConsumer<String, ? super OpenLineage.TagsJobFacet> consumer) {
    if (context.getOpenLineageConfig().getJob() == null) {
      return;
    }
    List<TagField> tags = context.getOpenLineageConfig().getJob().getTags();
    if (!tags.isEmpty()) {
      consumer.accept(
          "tags",
          this.context
              .getOpenLineage()
              .newTagsJobFacetBuilder()
              .tags(
                  tags.stream()
                      .map(
                          x ->
                              this.context
                                  .getOpenLineage()
                                  .newTagsJobFacetFields(x.getKey(), x.getValue(), x.getSource()))
                      .collect(Collectors.toList()))
              .build());
    }
  }
}
