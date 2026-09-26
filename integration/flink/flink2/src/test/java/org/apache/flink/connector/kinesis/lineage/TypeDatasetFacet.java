/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.flink.connector.kinesis.lineage;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/**
 * Test stub matching the fully-qualified class name of the Kinesis connector's type facet
 * (FLINK-39813). Lets {@code KinesisTypeDatasetFacetVisitor}'s reflective access be tested without
 * a dependency on the connector artifact.
 */
public class TypeDatasetFacet implements LineageDatasetFacet {

  public static final String TYPE_FACET_NAME = "type";

  private final TypeInformation<?> typeInformation;

  public TypeDatasetFacet(TypeInformation<?> typeInformation) {
    this.typeInformation = typeInformation;
  }

  public TypeInformation<?> getTypeInformation() {
    return typeInformation;
  }

  @Override
  public String name() {
    return TYPE_FACET_NAME;
  }
}
