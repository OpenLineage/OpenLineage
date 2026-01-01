/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

public class HivePropertiesExtension
    implements BeforeEachCallback, org.junit.jupiter.api.extension.AfterEachCallback {

  public static final String HIVECONF_SYSTEM_OVERRIDE_PREFIX = "hiveconf_";

  @Override
  public void beforeEach(ExtensionContext context) {
    context
        .getTestMethod()
        .ifPresent(
            method -> {
              HiveProperties properties = method.getAnnotation(HiveProperties.class);
              if (properties != null) {
                Store store = context.getStore(Namespace.create(getClass(), method));
                for (HiveProperty property : properties.value()) {
                  // Save original property to restore after test execution
                  store.put(
                      property.key(),
                      System.getProperty(HIVECONF_SYSTEM_OVERRIDE_PREFIX + property.key()));
                  System.setProperty(
                      HIVECONF_SYSTEM_OVERRIDE_PREFIX + property.key(), property.value());
                }
              }
            });
  }

  @Override
  public void afterEach(ExtensionContext context) {
    context
        .getTestMethod()
        .ifPresent(
            method -> {
              HiveProperties properties = method.getAnnotation(HiveProperties.class);
              if (properties != null) {
                Store store = context.getStore(Namespace.create(getClass(), method));
                for (HiveProperty property : properties.value()) {
                  String originalValue = store.remove(property.key(), String.class);
                  if (originalValue != null) {
                    System.setProperty(
                        HIVECONF_SYSTEM_OVERRIDE_PREFIX + property.key(), originalValue);
                  } else {
                    System.clearProperty(HIVECONF_SYSTEM_OVERRIDE_PREFIX + property.key());
                  }
                }
              }
            });
  }
}
