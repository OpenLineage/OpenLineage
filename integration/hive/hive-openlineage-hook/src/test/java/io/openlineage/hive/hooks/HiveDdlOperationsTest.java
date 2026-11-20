/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.jupiter.api.Test;

class HiveDdlOperationsTest {
  @Test
  void mapsTableDdlToDatasetLifecycle() {
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.CREATETABLE))
        .isEqualTo(LifecycleStateChange.CREATE);
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.DROPTABLE))
        .isEqualTo(LifecycleStateChange.DROP);
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.TRUNCATETABLE))
        .isEqualTo(LifecycleStateChange.TRUNCATE);
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.ALTERTABLE_RENAME))
        .isEqualTo(LifecycleStateChange.RENAME);
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.ALTERTABLE_ADDCOLS))
        .isEqualTo(LifecycleStateChange.ALTER);
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.ALTERTABLE_ADDPARTS))
        .isEqualTo(LifecycleStateChange.ALTER);
  }

  @Test
  void excludesNonTableOperations() {
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.CREATEDATABASE)).isNull();
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.QUERY)).isNull();
    assertThat(HiveDdlOperations.lifecycleState(HiveOperation.CREATEVIEW)).isNull();
  }
}
