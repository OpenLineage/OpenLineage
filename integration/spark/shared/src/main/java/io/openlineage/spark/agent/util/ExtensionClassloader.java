/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExtensionClassloader extends ClassLoader {

  private final Set<ClassLoader> classLoaders = new LinkedHashSet<>();

  public ExtensionClassloader(Collection<ClassLoader> aclassLoaders) {
    classLoaders.addAll(aclassLoaders);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    log.trace("Using multiple classloaders: {}", classLoaders);
    for (ClassLoader cl : classLoaders) {
      try {
        Class<?> aClass = cl.loadClass(name);
        log.trace("Classloader {} successfully loaded a class: {}", cl, aClass);
        return aClass;
      } catch (NoClassDefFoundError | Exception e) {
        log.error("Classloader {} failed to load {} class", cl, name, e);
      }
    }
    throw new ClassNotFoundException(name);
  }
}
