/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
public class JdbcLocation {
  @NonNull @Getter @Setter private String scheme;
  @Getter @Setter private Optional<String> authority;
  @Getter @Setter private Optional<String> instance;
  @Getter @Setter private Optional<String> database;

  public String toNamespace() {
    String result = scheme.toLowerCase(Locale.ROOT) + ":";
    if (authority.isPresent()) {
      result = String.format("%s//%s", result, authority.get().toLowerCase(Locale.ROOT));
    }
    if (instance.isPresent()) {
      result = String.format("%s/%s", result, StringUtils.stripStart(instance.get(), "/"));
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
