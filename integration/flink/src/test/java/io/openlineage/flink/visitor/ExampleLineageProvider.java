/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.LineageProvider;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ExampleLineageProvider implements LineageProvider<OpenLineage.InputDataset> {

  String name;
  String namespace;
  OpenLineage.SchemaDatasetFacet schemaDatasetFacet;

  @Override
  public List<OpenLineage.InputDataset> getDatasets(
      DatasetFactory<OpenLineage.InputDataset> datasetFactory) {
    OpenLineage.DatasetFacetsBuilder builder =
        datasetFactory.getDatasetFacetsBuilder().schema(schemaDatasetFacet);
    return Collections.singletonList(datasetFactory.getDataset(name, namespace, builder));
  }
}
