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
import lombok.NonNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

@Getter
public class UDTFExpr extends BaseExpr {
  private final GenericUDTF function;

  public UDTFExpr(@NonNull GenericUDTF function, @NonNull List<BaseExpr> children) {
    super(children);
    this.function = function;
  }

  @Override
  public String toString() {
    return String.format("UDTF (%s): [%s]", function, getChildren());
  }
}
