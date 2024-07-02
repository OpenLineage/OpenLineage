/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@AllArgsConstructor
public class JdbcLocation {
  @NonNull @Getter @Setter private String scheme;
  @NonNull @Getter @Setter private String authority;
  @Getter @Setter private Optional<String> instance;
  @Getter @Setter private Optional<String> database;

  public String toNamespace() {
    String result = String.format("%s://%s", scheme, authority);
    if (instance.isPresent()) {
      result = String.format("%s/%s", result, instance.get());
    }
    return result;
  }

  public String toName(List<String> parts) {
    if (database.isPresent()) {
      parts.add(0, database.get());
    }
    return String.join(".", parts);
  }
}
