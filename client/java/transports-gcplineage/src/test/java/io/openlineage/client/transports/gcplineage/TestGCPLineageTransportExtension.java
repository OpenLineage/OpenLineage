/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import com.google.cloud.datacatalog.lineage.v1.LineageSettings;

public class TestGCPLineageTransportExtension implements GCPLineageTransportExtension {
  public static boolean updateSettingsCalled = false;

  @Override
  public void updateSettings(LineageSettings.Builder builder) {
    updateSettingsCalled = true;
  }
}
