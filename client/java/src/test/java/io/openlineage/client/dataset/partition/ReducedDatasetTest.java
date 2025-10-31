/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.dataset.DatasetConfig;
import org.junit.jupiter.api.Test;

class ReducedDatasetTest {

  DatasetConfig config = DatasetConfig.defaultConfig();
  Dataset dataset = mock(Dataset.class);
  ReducedDataset reducedDataset;

  @Test
  void testTrimDatasetName() {
    verifyTrimDatasetName("/a/b", "/a/b/2025-01-01/c=1");
    verifyTrimDatasetName("/a/b", "/a/b/2025-01-01/c=1/d=1");
    verifyTrimDatasetName("/a/b", "/a/b/2025-01-01/c=1/d=1/20250101T34:34:34.123Z");
    verifyTrimDatasetName("/a/b", "/a/b/2025-01-01/2025-01-01");

    verifyTrimDatasetName("/a/b/c", "/a/b/c");
    verifyTrimDatasetName("/a/b/2025-01-01/2025-01-01/c", "/a/b/2025-01-01/2025-01-01/c");
    verifyTrimDatasetName("/a/b/2025T01T01", "/a/b/2025T01T01");
  }

  private void verifyTrimDatasetName(String expected, String input) {
    when(dataset.getName()).thenReturn(input);
    reducedDataset = ReducedDataset.of(config, dataset);
    assertThat(reducedDataset.getTrimmedDatasetName()).isEqualTo(expected);
  }
}
