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

import static io.openlineage.hive.parsing.ParsingTestUtils.createConstantExpr;
import static io.openlineage.hive.parsing.ParsingTestUtils.createParsedExpr;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.junit.jupiter.api.Test;

class UDTFExprTest {

  @Test
  void testUDTFExpr() {
    GenericUDTF function = new GenericUDTFExplode();
    List<BaseExpr> children = asList(createParsedExpr("array_column"), createConstantExpr("5"));
    UDTFExpr udtfExpr = new UDTFExpr(function, children);
    assertThat(udtfExpr.getFunction()).isEqualTo(function);
    assertThat(udtfExpr.getChildren()).isEqualTo(children);
    assertThat(udtfExpr.toString())
        .isEqualTo("UDTF (GenericUDTFExplode): [Column[array_column], Const string 5]");
  }
}
