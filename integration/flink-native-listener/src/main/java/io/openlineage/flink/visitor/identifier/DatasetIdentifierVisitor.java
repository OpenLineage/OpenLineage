/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Collection;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/**
 * Interface for methods allowing translating Flink lineage facets into OpenLineage dataset
 * identifiers.
 */
public interface DatasetIdentifierVisitor {

  /**
   * Determines if visitor can be applied for a given dataset
   *
   * @param dataset
   * @return
   */
  boolean isDefinedAt(LineageDataset dataset);

  /**
   * Returns dataset names based on Flink's lineage dataset. When returning more than one dataset
   * identifier, a dataset returned from Flink is exploded into several datasets.
   *
   * @param dataset
   * @return
   */
  Collection<DatasetIdentifier> apply(LineageDataset dataset);
}
