/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import scala.runtime.AbstractPartialFunction;

public abstract class OpenLineageAbstractPartialFunction<T, D>
    extends AbstractPartialFunction<T, D> {
  abstract String appliedName();
}
