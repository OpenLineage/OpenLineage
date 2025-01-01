/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import javax.annotation.Nullable;
import lombok.NoArgsConstructor;

/** An exception thrown to indicate a client error. */
@NoArgsConstructor
public class OpenLineageClientException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructs a {@code OpenLineageClientException} with the message {@code message}.
   *
   * @param message message
   */
  public OpenLineageClientException(@Nullable final String message) {
    super(message);
  }

  /**
   * Constructs a {@code OpenLineageClientException} with the cause {@code cause}.
   *
   * @param cause cause
   */
  public OpenLineageClientException(@Nullable final Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a {@code OpenLineageClientException} with the message {@code message} and the cause
   * {@code cause}.
   *
   * @param message message
   * @param cause cause
   */
  public OpenLineageClientException(
      @Nullable final String message, @Nullable final Throwable cause) {
    super(message, cause);
  }
}
