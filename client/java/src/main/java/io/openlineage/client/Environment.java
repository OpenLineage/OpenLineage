/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.util.Map;

// This class exists because it's not possible to mock System
public class Environment {
  public static String getEnvironmentVariable(String key) {
    return System.getenv(key);
  }

  // get all envinroment variables
  public static Map<String, String> getAllEnvironmentVariables() {
    return System.getenv();
  }
}
