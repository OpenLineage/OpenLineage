/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.hooks;

import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/** Table operations that change a dataset's lifecycle or metadata. */
final class HiveDdlOperations {
  private HiveDdlOperations() {}

  static LifecycleStateChange lifecycleState(HiveOperation operation) {
    switch (operation) {
      case CREATETABLE:
        return LifecycleStateChange.CREATE;
      case DROPTABLE:
        return LifecycleStateChange.DROP;
      case TRUNCATETABLE:
        return LifecycleStateChange.TRUNCATE;
      case ALTERTABLE_RENAME:
        return LifecycleStateChange.RENAME;
      case ALTERTABLE_ADDCOLS:
      case ALTERTABLE_REPLACECOLS:
      case ALTERTABLE_RENAMECOL:
      case ALTERTABLE_LOCATION:
      case ALTERPARTITION_LOCATION:
      case ALTERTABLE_ADDPARTS:
        return LifecycleStateChange.ALTER;
      default:
        return null;
    }
  }
}
