/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.spark.rdd.RDD;
import org.junit.jupiter.api.Test;

class RddsTest {
  @Test
  void testFindFileLikeRddsWhenDependeciesAreNull() {
    RDD<?> rdd = mock(RDD.class);
    when(rdd.getDependencies()).thenReturn(null);

    assertThat(Rdds.findFileLikeRdds(rdd)).isEmpty();
  }
}
