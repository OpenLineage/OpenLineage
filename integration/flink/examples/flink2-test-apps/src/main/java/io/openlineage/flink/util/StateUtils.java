/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;

public final class StateUtils {

  private StateUtils() {}

  public static <T> T value(ValueState<T> state, T defaultValue) throws IOException {
    T value = state.value();
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }
}
