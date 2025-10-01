/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import io.openlineage.client.dataset.partition.trimmer.DateTrimmer;
import io.openlineage.client.dataset.partition.trimmer.MultiDirDateTrimmer;
import org.junit.jupiter.api.Test;

class DatasetConfigTest {

  DatasetConfig config = new DatasetConfig();

  @Test
  void testGetDatasetNameTrimmers() {
    assertThat(config.getDatasetNameTrimmers()).hasSize(4);
  }

  @Test
  void testDisabledTrimmers() {
    config.setDisabledTrimmers(
        DateTrimmer.class.getName() + ";" + MultiDirDateTrimmer.class.getName());
    assertThat(config.getDatasetNameTrimmers()).hasSize(2);
  }

  @Test
  void testEnabledTrimmers() {
    config.setExtraTrimmers(ExtraTrimmer.class.getName() + ";io.openlineage.NonExistingTrimmer");
    assertThat(config.getDatasetNameTrimmers()).hasSize(5);
  }

  static class ExtraTrimmer implements DatasetNameTrimmer {
    @Override
    public boolean canTrim(String name) {
      return false;
    }
  }
}
