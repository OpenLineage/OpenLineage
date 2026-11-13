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
    List<Throwable> failures = new ArrayList<>();
    for (ClassLoader cl : classLoaders) {
      try {
        Class<?> aClass = cl.loadClass(name);
        log.trace("Classloader {} successfully loaded a class: {}", cl, aClass);
        return aClass;
      } catch (NoClassDefFoundError | Exception e) {
        // expected whenever a later classloader can see the class
        log.debug("Classloader {} failed to load {} class", cl, name);
        failures.add(e);
      }
    }
    ClassNotFoundException notFound = new ClassNotFoundException(name);
    failures.forEach(notFound::addSuppressed);
    // ServiceLoader replaces this exception with one which has no cause, so log it here
    log.warn("None of the classloaders {} could load {} class", classLoaders, name, notFound);
    throw notFound;
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
