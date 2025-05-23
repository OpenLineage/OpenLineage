/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.ql.metadata.Table;

@Getter
@Setter
public class ColumnExpr extends BaseExpr {

  private int index = -1;
  private String name;
  private Table table;
  private List<QueryExpr> queries;
  private BaseExpr expression;

  public ColumnExpr(String name) {
    super();
    this.name = name;
  }

  @Override
  public String toString() {
    return String.format("Column[%s]", name);
  }
}
