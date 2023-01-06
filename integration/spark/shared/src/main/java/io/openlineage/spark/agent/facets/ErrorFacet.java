/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.Builder;
import lombok.NonNull;

public class ErrorFacet extends OpenLineage.DefaultRunFacet {
  @SuppressWarnings("PMD")
  private final Exception exception;

  @Builder
  public ErrorFacet(@NonNull Exception exception) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.exception = exception;
  }

  public String getMessage() {
    return exception.getMessage();
  }

  public String getStackTrace() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    exception.printStackTrace(pw);
    return sw.toString();
  }
}
