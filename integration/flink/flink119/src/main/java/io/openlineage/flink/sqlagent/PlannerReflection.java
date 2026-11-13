/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import java.lang.reflect.Method;

/** Reflects only objects from the live planner; no Calcite dependency enters the agent. */
final class PlannerReflection {
  static boolean has(Object target, String name) {
    for (Method method : target.getClass().getMethods())
      if (method.getName().equals(name)) return true;
    return false;
  }

  static Object member(Object target, String name) throws Exception {
    java.lang.reflect.Field field = target.getClass().getField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  static Object call(Object target, String name, Object... args) throws Exception {
    for (Method method : target.getClass().getMethods()) {
      if (!method.getName().equals(name) || method.getParameterCount() != args.length) continue;
      Class<?>[] types = method.getParameterTypes();
      boolean matches = true;
      for (int i = 0; i < args.length; i++) {
        Class<?> type = types[i] == int.class ? Integer.class : types[i];
        if (args[i] != null && !type.isInstance(args[i])) matches = false;
      }
      if (!matches) continue;
      method.setAccessible(true);
      return method.invoke(target, args);
    }
    throw new NoSuchMethodException(target.getClass().getName() + "." + name);
  }
}
