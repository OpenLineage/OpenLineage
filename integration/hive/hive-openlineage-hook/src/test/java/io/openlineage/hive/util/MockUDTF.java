/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.util;

import io.openlineage.hive.udf.interfaces.UDFAdditionalLineage;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class MockUDTF extends GenericUDTF implements UDFAdditionalLineage {

  Boolean isMasking;

  public MockUDTF(Boolean isMasking) {
    super();
    this.isMasking = isMasking;
  }

  public void close() throws HiveException {}

  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    ArrayList<String> fieldNames = new ArrayList();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList();

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  public void process(Object[] o) throws HiveException {
    return;
  }

  public String toString() {
    return "MockUDTF";
  }

  @Override
  public Boolean isMasking() {
    return isMasking;
  }
}
