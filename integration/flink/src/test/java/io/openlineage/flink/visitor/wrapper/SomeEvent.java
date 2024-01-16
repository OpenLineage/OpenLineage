/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class SomeEvent implements SpecificRecord {

  @Override
  public void put(int i, Object v) {}

  @Override
  public Object get(int i) {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
