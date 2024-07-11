/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1;

import io.openlineage.client.utils.DatasetIdentifier;

/** Dataset with an identifier containing namespace and name */
interface DatasetWithIdentifier {
  DatasetIdentifier getDatasetIdentifier();
}
