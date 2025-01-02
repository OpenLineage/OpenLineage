/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.Collections;

public class DatasetUtil {

  /**
   * Enriches the existing dataset with the column lineage facet.
   *
   * @param openLineage
   * @param dataset
   * @param columnLineageDatasetFacet
   * @return
   */
  public static OutputDataset enrichDataset(
      OpenLineage openLineage,
      OutputDataset dataset,
      ColumnLineageDatasetFacet columnLineageDatasetFacet) {
    return openLineage
        .newOutputDatasetBuilder()
        .namespace(dataset.getNamespace())
        .name(dataset.getName())
        .outputFacets(dataset.getOutputFacets())
        .facets(
            OpenLineageClientUtils.mergeFacets(
                Collections.singletonMap("columnLineage", columnLineageDatasetFacet),
                dataset.getFacets(),
                DatasetFacets.class))
        .build();
  }
}
