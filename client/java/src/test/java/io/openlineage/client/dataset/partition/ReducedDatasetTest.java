/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.dataset.DatasetConfig;
import java.util.Optional;
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

  @Test
  void testReduceDatasetsWithNullFacets() {
    InputDataset d1 = mock(InputDataset.class);
    InputDataset d2 = mock(InputDataset.class);
    when(d1.getName()).thenReturn("/a/b/2025-01-01");
    when(d2.getName()).thenReturn("/a/b/2025-01-02");

    Optional<ReducedDataset> reduced =
        ReducedDataset.of(config, d1).reduce(ReducedDataset.of(config, d2));

    assertThat(reduced).isPresent();
    assertThat(reduced.get().getTrimmedDatasetName()).isEqualTo("/a/b");
    assertThat(reduced.get().getNonTrimmedNames())
        .containsExactly("/a/b/2025-01-01", "/a/b/2025-01-02");
  }

  @Test
  void testReduceDatasetWithNullFacetsAndDatasetWithNonNullFacets() {
    InputDataset withNullFacets = mock(InputDataset.class);
    InputDataset withFacets = mock(InputDataset.class);
    when(withNullFacets.getName()).thenReturn("/a/b/2025-01-01");
    when(withFacets.getName()).thenReturn("/a/b/2025-01-02");
    when(withFacets.getFacets()).thenReturn(mock(DatasetFacets.class));

    assertThat(
            ReducedDataset.of(config, withNullFacets).reduce(ReducedDataset.of(config, withFacets)))
        .isEmpty();
    assertThat(
            ReducedDataset.of(config, withFacets).reduce(ReducedDataset.of(config, withNullFacets)))
        .isEmpty();
  }

  private void verifyTrimDatasetName(String expected, String input) {
    when(dataset.getName()).thenReturn(input);
    reducedDataset = ReducedDataset.of(config, dataset);
    assertThat(reducedDataset.getTrimmedDatasetName()).isEqualTo(expected);
  }
}
