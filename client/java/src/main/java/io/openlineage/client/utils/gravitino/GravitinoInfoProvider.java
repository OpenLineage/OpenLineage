/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

public interface GravitinoInfoProvider {
  boolean isAvailable();

  GravitinoInfo getGravitinoInfo();
}
