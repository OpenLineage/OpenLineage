/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ProcessingEngineRunFacet;
import org.apache.spark.SparkContext;

/**
 * This class is the one that actually does the work of building an instance of {@link
 * ProcessingEngineRunFacet}. The reason for the name is to:
 *
 * <p>
 *
 * <ol>
 *   <li>Alert the reader that it is related to {@link SparkProcessingEngineRunFacetBuilder}, and;
 *   <li>Prevent inadvertent name collisions between it and the actual {@link
 *       ProcessingEngineRunFacet} resulting in incredibly long lines.
 * </ol>
 */
public final class SparkProcessingEngineRunFacetBuilderDelegate {
  private final OpenLineage ol;
  private final String version;

  public SparkProcessingEngineRunFacetBuilderDelegate(OpenLineage ol, SparkContext sparkContext) {
    this.ol = ol;
    this.version = sparkContext.version();
  }

  public SparkProcessingEngineRunFacetBuilderDelegate(OpenLineage ol, String version) {
    this.ol = ol;
    this.version = version;
  }

  public ProcessingEngineRunFacet buildFacet() {
    // TODO: At some stage, we probably need to create a canonical dictionary for the names of the
    //  processing engines, or define a convention - i.e., names of processing engines should be
    //  snake_case
    return ol.newProcessingEngineRunFacetBuilder()
        .name("spark")
        .version(version)
        .openlineageAdapterVersion(this.getClass().getPackage().getImplementationVersion())
        .build();
  }
}
