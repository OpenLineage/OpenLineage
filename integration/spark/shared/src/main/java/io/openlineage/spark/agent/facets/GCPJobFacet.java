/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Map;

public class GCPJobFacet extends OpenLineage.DefaultJobFacet {

  public GCPJobFacet(Map<String, Object> properties) {
    this(properties, null);
  }

  public GCPJobFacet(Map<String, Object> properties, Boolean _deleted) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI, _deleted);
    this.getAdditionalProperties().putAll(properties);
  }
}
