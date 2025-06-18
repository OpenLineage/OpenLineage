/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.hive.client.Versions;
import lombok.Getter;

/** Captures information related to the Hive query. */
@Getter
public class HiveQueryInfoFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("queryId")
  private String queryId;

  @JsonProperty("operationName")
  private String operationName;

  public HiveQueryInfoFacet() {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
  }

  public HiveQueryInfoFacet setQueryId(String queryId) {
    this.queryId = queryId;
    return this;
  }

  public HiveQueryInfoFacet setOperationName(String operationName) {
    this.operationName = operationName;
    return this;
  }
}
