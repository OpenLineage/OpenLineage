/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.hive.client.Versions;
import lombok.Getter;

/**
 * Captures information related to the Hive session, ich cannot be changed after session was
 * created.
 */
@Getter
public class HiveSessionInfoFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("username")
  private String username;

  @JsonProperty("clientIp")
  private String clientIp;

  @JsonProperty("sessionId")
  private String sessionId;

  public HiveSessionInfoFacet() {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
  }

  public HiveSessionInfoFacet setUsername(String username) {
    this.username = username;
    return this;
  }

  public HiveSessionInfoFacet setClientIp(String clientIp) {
    this.clientIp = clientIp;
    return this;
  }

  public HiveSessionInfoFacet setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }
}
