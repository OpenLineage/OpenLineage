/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class WrapperUtilsTest {

  private static final String SOME_STRING = "some-string";
  AccessedClass object = new AccessedClass(SOME_STRING);

  @Test
  void testGetFieldValue() {
    assertEquals(
        Optional.of(SOME_STRING), WrapperUtils.getFieldValue(object.getClass(), object, "field"));
  }

  @Test
  void testGetFieldValueIncorrectField() {
    assertEquals(
        Optional.empty(), WrapperUtils.getFieldValue(object.getClass(), object, "other-field"));
  }

  @Test
  void testInvoke() {
    assertEquals(
        Optional.of(SOME_STRING), WrapperUtils.invoke(AccessedClass.class, object, "getField"));
  }

  @Test
  void testInvokeWrongMethod() {
    assertEquals(
        Optional.empty(), WrapperUtils.invoke(AccessedClass.class, object, "wrong-method"));
  }

  @Test
  void testInvokeStatic() {
    assertEquals(Optional.of(SOME_STRING), WrapperUtils.invokeStatic(this.getClass(), "getStatic"));
  }

  @Test
  void testInvokeStaticWrongMethod() {
    assertEquals(Optional.empty(), WrapperUtils.invokeStatic(this.getClass(), "wrong-method"));
  }

  class AccessedClass {
    private final String field;

    AccessedClass(String field) {
      this.field = field;
    }

    private String getField() { // NOPMD
      return field;
    }
  }

  public static String getStatic() {
    return SOME_STRING;
  }
}
