/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.util.DeltaUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.CustomColumnLineageVisitor;
import java.util.stream.Stream;

public class VisitorLoader {

  public static Stream<CustomColumnLineageVisitor> load(OpenLineageContext context) {
    if (DeltaUtils.hasMergeIntoClasses()) {
      return Stream.of(new MergeIntoDeltaColumnLineageVisitor(context));
    } else {
      return Stream.empty();
    }
  }
}
