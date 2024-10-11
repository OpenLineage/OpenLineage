/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.parsing;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

@Getter
@Setter
public class ColumnExpr extends ExprNodeDesc {
  private static final long serialVersionUID = 1L;

  private int index = -1;
  private String name;
  private Table table;
  private List<QueryExpr> queries;
  private ExprNodeDesc expression;

  @Override
  @SuppressWarnings("PMD.CloneMethodMustImplementCloneable")
  public ColumnExpr clone() {
    // Not implemented because not needed for our purposes
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSame(Object o) {
    // Not implemented because not needed for our purposes
    throw new UnsupportedOperationException();
  }

  @Override
  public String getExprString() {
    return String.format("Column: %s", name);
  }
}
