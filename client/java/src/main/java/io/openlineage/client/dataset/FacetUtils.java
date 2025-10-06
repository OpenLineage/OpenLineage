/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;

public class FacetUtils {

  public static InputDatasetInputFacetsBuilder toBuilder(
      OpenLineage openLineage, OpenLineage.InputDatasetInputFacets facets) {
    InputDatasetInputFacetsBuilder builder = openLineage.newInputDatasetInputFacetsBuilder();
    builder
        .dataQualityAssertions(facets.getDataQualityAssertions())
        .dataQualityMetrics(facets.getDataQualityMetrics())
        .inputStatistics(facets.getInputStatistics())
        .iceberg_scan_report(facets.getIceberg_scan_report());

    facets.getAdditionalProperties().forEach(builder::put);

    return builder;
  }

  public static OutputDatasetOutputFacetsBuilder toBuilder(
      OpenLineage openLineage, OpenLineage.OutputDatasetOutputFacets facets) {
    OutputDatasetOutputFacetsBuilder builder = openLineage.newOutputDatasetOutputFacetsBuilder();
    builder
        .outputStatistics(facets.getOutputStatistics())
        .iceberg_commit_report(facets.getIceberg_commit_report());

    facets.getAdditionalProperties().forEach(builder::put);

    return builder;
  }
}
