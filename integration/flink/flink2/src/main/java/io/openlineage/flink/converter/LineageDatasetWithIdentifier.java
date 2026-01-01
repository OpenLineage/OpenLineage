/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.utils.DatasetIdentifier;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.flink.streaming.api.lineage.LineageDataset;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class LineageDatasetWithIdentifier {

  private final DatasetIdentifier datasetIdentifier;
  private final LineageDataset flinkDataset;
}
