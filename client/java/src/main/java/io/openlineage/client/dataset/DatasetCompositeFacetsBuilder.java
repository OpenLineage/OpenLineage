/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;
import io.openlineage.client.OpenLineage.OutputDatasetOutputFacetsBuilder;
import lombok.Getter;

/** Builder class to include both dataset and input/output facets in a single class. */
public class DatasetCompositeFacetsBuilder<D extends Dataset> {
  private OpenLineage openLineage;

  @Getter private OpenLineage.DatasetFacetsBuilder facets;

  @Getter private InputDatasetInputFacetsBuilder inputFacets;

  @Getter private OutputDatasetOutputFacetsBuilder outputFacets;

  public DatasetCompositeFacetsBuilder(OpenLineage openLineage) {
    this.openLineage = openLineage;

    this.facets = this.openLineage.newDatasetFacetsBuilder();
    this.inputFacets = this.openLineage.newInputDatasetInputFacetsBuilder();
    this.outputFacets = this.openLineage.newOutputDatasetOutputFacetsBuilder();
  }
}
