/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.util;

import io.openlineage.hive.udf.interfaces.UDFAdditionalLineage;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class MockUDF extends GenericUDF implements UDFAdditionalLineage {

  Boolean isMasking;

  public MockUDF(Boolean isMasking) {
    super();
    this.isMasking = isMasking;
  }

  public Text evaluate(Text input) {
    return input;
  }

  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Boolean isMasking() {
    return isMasking;
  }

  public Object evaluate(DeferredObject[] arg0) {
    return arg0;
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "MOCK";
  }
}
