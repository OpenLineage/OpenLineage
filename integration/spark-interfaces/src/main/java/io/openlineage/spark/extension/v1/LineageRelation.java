/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.v1;

import io.openlineage.client.utils.DatasetIdentifier;

/**
 * Interface to be implemented for extension's classes extending
 * `org.apache.spark.sql.sources.BaseRelation`. Implementing it allows extracting lineage from such
 * objects. Implementing `getNamespace` and `getName` within the `DatasetIdentifier` is obligatory.
 */
public interface LineageRelation {
  DatasetIdentifier getLineageDatasetIdentifier(OpenLineageExtensionContext openLineageContext);
}
