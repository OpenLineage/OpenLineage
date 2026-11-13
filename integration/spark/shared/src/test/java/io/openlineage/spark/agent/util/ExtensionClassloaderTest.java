/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ExtensionClassloaderTest {

  @Test
  void testLoadClassFallsThroughToNextClassloader() throws Exception {
    try (URLClassLoader empty = new URLClassLoader(new URL[0], null)) {
      ExtensionClassloader classloader =
          new ExtensionClassloader(Arrays.asList(empty, getClass().getClassLoader()));

      assertThat(classloader.loadClass(ExtensionClassloaderTest.class.getName()))
          .isSameAs(ExtensionClassloaderTest.class);
    }
  }

  @Test
  void testLoadClassReportsEveryClassloaderFailure() throws Exception {
    try (URLClassLoader first = new URLClassLoader(new URL[0], null);
        URLClassLoader second = new URLClassLoader(new URL[0], null)) {
      ExtensionClassloader classloader = new ExtensionClassloader(Arrays.asList(first, second));

      assertThatThrownBy(() -> classloader.loadClass("com.example.Missing"))
          .isInstanceOf(ClassNotFoundException.class)
          .satisfies(e -> assertThat(e.getSuppressed()).hasSize(2));
    }
  }
}
