/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.net.URI;

public class Events {
  public static OpenLineage.RunEvent event() {
    return new OpenLineage(URI.create("http://test.producer")).newRunEventBuilder().build();
  }
}
