/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDatasetInputFacetsBuilder;

public interface InputDatasetWithFacets {
  DatasetFacetsBuilder getDatasetFacetsBuilder();

  InputDatasetInputFacetsBuilder getInputFacetsBuilder();
}
