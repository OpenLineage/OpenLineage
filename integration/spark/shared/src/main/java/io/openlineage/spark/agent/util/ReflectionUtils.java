/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;

@Slf4j
public class ReflectionUtils {
  public static Optional<Object> tryExecuteStaticMethodForClassName(
      String className, String methodName, Object... args) {
    Class<?> clazz;
    try {
      clazz = ClassUtils.getClass(className);
    } catch (ClassNotFoundException | Error e) {
      log.debug("Can't get class {}", className, e);
      return Optional.empty();
    }
    args = ArrayUtils.nullToEmpty(args);
    Class<?>[] parameterTypes = ClassUtils.toClass(args);
    try {
      return Optional.of(MethodUtils.invokeStaticMethod(clazz, methodName, args, parameterTypes));
    } catch (Error | Exception e) {
      log.debug("Can't execute static method {}.{}:", className, methodName, e);
      return Optional.empty();
    }
  }

  public static Optional<Object> tryExecuteMethod(
      Object object, String methodName, Object... args) {
    try {
      return Optional.of(MethodUtils.invokeMethod(object, methodName, args));
    } catch (Exception exception) {
      return Optional.empty();
    }
  }

  public static boolean hasClass(String aClass) {
    try {
      ReflectionUtils.class.getClassLoader().loadClass(aClass);
      return true;
    } catch (Exception e) {
      //
      // we don't care
    }
    return false;
  }

  public static boolean hasClasses(String... classes) {
    return Arrays.stream(classes).allMatch(ReflectionUtils::hasClass);
  }
}
