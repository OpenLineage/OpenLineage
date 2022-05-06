package io.openlineage.flink.visitor.wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class WrapperUtilsTest {

  AccessedClass object = new AccessedClass("some-string");

  @Test
  public void testGetFieldValue() {
    assertEquals(
        Optional.of("some-string"), WrapperUtils.getFieldValue(object.getClass(), object, "field"));
  }

  @Test
  public void testGetFieldValueIncorrectField() {
    assertEquals(
        Optional.empty(), WrapperUtils.getFieldValue(object.getClass(), object, "other-field"));
  }

  @Test
  public void testInvoke() {
    assertEquals(
        Optional.of("some-string"), WrapperUtils.invoke(AccessedClass.class, object, "getField"));
  }

  @Test
  public void testInvokeWrongMethod() {
    assertEquals(
        Optional.empty(), WrapperUtils.invoke(AccessedClass.class, object, "wrong-method"));
  }

  @Test
  public void testInvokeStatic() {
    assertEquals(
        Optional.of("some-string"), WrapperUtils.invokeStatic(this.getClass(), "getStatic"));
  }

  @Test
  public void testInvokeStaticWrongMethod() {
    assertEquals(Optional.empty(), WrapperUtils.invokeStatic(this.getClass(), "wrong-method"));
  }

  class AccessedClass {
    private final String field;

    AccessedClass(String field) {
      this.field = field;
    }

    private String getField() {
      return field;
    }
  }

  public static String getStatic() {
    return "some-string";
  }
}
