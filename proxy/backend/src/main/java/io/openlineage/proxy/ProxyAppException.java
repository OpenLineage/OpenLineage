/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy;

import javax.annotation.Nullable;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ProxyAppException extends Exception {
  private static final long serialVersionUID = 1L;

  public ProxyAppException(@Nullable final String message) {
    super(message);
  }

  public ProxyAppException(@Nullable final Throwable cause) {
    super(cause);
  }

  public ProxyAppException(@Nullable final String message, @Nullable final Throwable cause) {
    super(message, cause);
  }
}
