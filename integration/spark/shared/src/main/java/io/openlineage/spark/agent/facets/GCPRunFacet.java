/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Map;

public class GCPRunFacet extends OpenLineage.DefaultRunFacet {
  public GCPRunFacet(Map<String, Object> properties) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.getAdditionalProperties().putAll(properties);
  }
}
