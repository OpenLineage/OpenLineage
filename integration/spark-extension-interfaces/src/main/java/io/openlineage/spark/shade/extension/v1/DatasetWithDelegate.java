/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

/** Dataset with a node in LogicalPlan where a input dataset shall be extracted from */
interface DatasetWithDelegate {
  Object getNode();
}
