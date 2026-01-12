/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import com.google.common.collect.Iterators;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
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

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    List<Enumeration<URL>> enumerations = new ArrayList<>();

    for (ClassLoader classLoader : classLoaders) {
      enumerations.add(classLoader.getResources(name));
    }

    return Iterators.asEnumeration(
        Iterators.concat(enumerations.stream().map(Iterators::forEnumeration).iterator()));
  }
}
