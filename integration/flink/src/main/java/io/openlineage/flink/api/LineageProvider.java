/*
/* Copyright 2018-2032 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.api;

import io.openlineage.client.OpenLineage;
import java.util.List;

/**
 * This is the interface for custom operators that want to expose their lineage information to
 * OpenLineage plugin. Sources should extend this interface with OpenLineage.InputDataset, while
 * sinks should use OpenLineage.OutputDataset.
 *
 * <p>To construct the dataset, use DatasetFactory.getDataset method. You can use
 * DatasetFactory.getDatasetFacetsBuilder to get OpenLineage.DatasetFacetsBuilder that will allow
 * you to attach additional information (facets): schema, exact dataset version, etc. See
 * OpenLineage schema to see what additional facets you can attach.
 *
 * <p>If your connector writes to or reads from multiple tables, multiple topics, or reads multiple
 * Iceberg datasets, it's recommended to return multiple datasets.
 */
public interface LineageProvider<D extends OpenLineage.Dataset> {

  List<D> getDatasets(DatasetFactory<D> datasetFactory);
}
