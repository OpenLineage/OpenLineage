/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
