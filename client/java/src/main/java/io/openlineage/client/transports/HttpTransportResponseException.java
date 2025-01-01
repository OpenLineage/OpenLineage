/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.*;
import lombok.*;

/** An exception thrown to indicate a client error relating to a http response. */
@Getter
public class HttpTransportResponseException extends OpenLineageClientException {
  private static final long serialVersionUID = 1L;

  private final int statusCode;
  private final String body;

  public HttpTransportResponseException(int statusCode, String body) {
    super(String.format("code: %d, response: %s", statusCode, body), null);
    this.statusCode = statusCode;
    this.body = body;
  }
}
