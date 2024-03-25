/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.OwnershipJobFacet;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} that adds the {@link OwnershipJobFacet} to a job. This facet is
 * generated for every {@link SparkListenerEvent}.
 */
public class OwnershipJobFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, OwnershipJobFacet> {
  private final OpenLineageContext olContext;

  public OwnershipJobFacetBuilder(OpenLineageContext olContext) {
    this.olContext = olContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return super.isDefinedAt(x);
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super OwnershipJobFacet> consumer) {
    if (olContext.getOpenLineageYaml() == null
        || olContext.getOpenLineageYaml().getJobConfig() == null
        || olContext.getOpenLineageYaml().getJobConfig().getJobOwners() == null) {
      return;
    }

    List<OwnershipJobFacetOwners> ownersList = new ArrayList<>();
    olContext
        .getOpenLineageYaml()
        .getJobConfig()
        .getJobOwners()
        .forEach(
            (type, name) ->
                ownersList.add(
                    olContext
                        .getOpenLineage()
                        .newOwnershipJobFacetOwnersBuilder()
                        .name(name)
                        .type(type)
                        .build()));
    consumer.accept(
        "ownership",
        olContext.getOpenLineage().newOwnershipJobFacetBuilder().owners(ownersList).build());
  }
}
