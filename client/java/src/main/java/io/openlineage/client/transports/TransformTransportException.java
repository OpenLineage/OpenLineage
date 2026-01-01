/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class TransformTransportException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public TransformTransportException(String message) {
    super(message);
  }

  public TransformTransportException(String message, Throwable cause) {
    super(message, cause);
  }
}
