/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@AllArgsConstructor
public class JdbcLocation {
  @NonNull @Getter @Setter private String scheme;
  @NonNull @Getter @Setter private String host;
  @Getter @Setter private Optional<String> port;
  @Getter @Setter private Optional<String> instance;
  @Getter @Setter private Optional<String> database;

  public String getNamespace() {
    String result = String.format("%s://%s", scheme, host);
    if (port.isPresent()) {
      result = String.format("%s:%s", result, port.get());
    }
    if (instance.isPresent()) {
      result = String.format("%s/%s", result, instance.get());
    }
    return result;
  }
}
