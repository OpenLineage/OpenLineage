/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import lombok.Getter;

/**
 * Deserialization target for the {@code spark.openlineage.context} config property: a JSON payload
 * for propagating parent-run context, shaped as a values-only subset of {@link
 * OpenLineage.ParentRunFacet} (no {@code _producer}/{@code _schemaURL} envelope), wrapped in a
 * {@code parent} key.
 */
@Getter
public class ParentRunContext {
  @JsonProperty("parent")
  private ParentRun parent;

  @Getter
  public static class ParentRun {
    @JsonProperty("run")
    private OpenLineage.ParentRunFacetRun run;

    @JsonProperty("job")
    private OpenLineage.ParentRunFacetJob job;

    @JsonProperty("root")
    private OpenLineage.ParentRunFacetRoot root;
  }
}
