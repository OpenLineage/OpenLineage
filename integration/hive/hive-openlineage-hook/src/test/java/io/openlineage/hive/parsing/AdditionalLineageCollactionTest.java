/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.hive.util.MockUDAF;
import io.openlineage.hive.util.MockUDF;
import io.openlineage.hive.util.MockUDTF;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AdditionalLineageCollactionTest {

  @Test
  void testAdditionalLineageFunction() {
    MockUDF maskingMock = new MockUDF(true);
    MockUDF nonMaskingMock = new MockUDF(false);
    List<BaseExpr> children = Collections.singletonList(new ColumnExpr("x"));
    FunctionExpr maskingFunction = new FunctionExpr("maskingMock", maskingMock, children);
    FunctionExpr nonMaskingFunction = new FunctionExpr("nonMaskingMock", nonMaskingMock, children);
    assertThat(ColumnLineageCollector.isMasking(maskingFunction)).isEqualTo(true);
    assertThat(ColumnLineageCollector.isMasking(nonMaskingFunction)).isEqualTo(false);
  }

  @Test
  void testAdditionalLineageAggregation() {
    MockUDAF maskingMock = new MockUDAF(true);
    MockUDAF nonMaskingMock = new MockUDAF(false);
    List<BaseExpr> children = Collections.singletonList(new ColumnExpr("x"));
    AggregateExpr maskingFunction = new AggregateExpr(maskingMock, children);
    AggregateExpr nonMaskingFunction = new AggregateExpr(nonMaskingMock, children);
    assertThat(ColumnLineageCollector.isMasking(maskingFunction)).isEqualTo(true);
    assertThat(ColumnLineageCollector.isMasking(nonMaskingFunction)).isEqualTo(false);
  }

  @Test
  void testAdditionalLineageUDTF() {
    MockUDTF maskingMock = new MockUDTF(true);
    MockUDTF nonMaskingMock = new MockUDTF(false);
    List<BaseExpr> children = Collections.singletonList(new ColumnExpr("x"));
    UDTFExpr maskingFunction = new UDTFExpr(maskingMock, children);
    UDTFExpr nonMaskingFunction = new UDTFExpr(nonMaskingMock, children);
    assertThat(ColumnLineageCollector.isMasking(maskingFunction)).isEqualTo(true);
    assertThat(ColumnLineageCollector.isMasking(nonMaskingFunction)).isEqualTo(false);
  }
}
