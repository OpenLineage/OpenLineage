/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class StarExpr extends BaseExpr {

  private final List<String> aliases;

  public StarExpr(@NonNull List<String> aliases, @NonNull List<BaseExpr> children) {
    super(children);
    this.aliases = aliases;
  }
}
