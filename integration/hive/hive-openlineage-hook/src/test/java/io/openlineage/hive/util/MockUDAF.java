/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.util;

import io.openlineage.hive.udf.interfaces.UDFAdditionalLineage;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class MockUDAF extends AbstractGenericUDAFResolver implements UDFAdditionalLineage {

  Boolean isMasking;

  public MockUDAF(Boolean isMasking) {
    super();
    this.isMasking = isMasking;
  }

  @Override
  public Boolean isMasking() {
    return isMasking;
  }

  public static class MockUDAFEvaluator extends GenericUDAFEvaluator {

    static class MockBuffer implements AggregationBuffer {}

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new MockUDAF.MockUDAFEvaluator.MockBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) {}

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {}

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      return null;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {}

    @Override
    public Object terminate(AggregationBuffer agg) {
      return null;
    }
  }
}
