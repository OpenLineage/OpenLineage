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

    if (facets == null) return builder;

    builder
        .dataQualityAssertions(facets.getDataQualityAssertions())
        .dataQualityMetrics(facets.getDataQualityMetrics())
        .inputStatistics(facets.getInputStatistics())
        .icebergScanReport(facets.getIcebergScanReport());

    facets.getAdditionalProperties().forEach(builder::put);

    return builder;
  }

  public static OutputDatasetOutputFacetsBuilder toBuilder(
      OpenLineage openLineage, OpenLineage.OutputDatasetOutputFacets facets) {
    OutputDatasetOutputFacetsBuilder builder = openLineage.newOutputDatasetOutputFacetsBuilder();

    if (facets == null) return builder;

    builder
        .outputStatistics(facets.getOutputStatistics())
        .icebergCommitReport(facets.getIcebergCommitReport());

    facets.getAdditionalProperties().forEach(builder::put);

    return builder;
  }
}
