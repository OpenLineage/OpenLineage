/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import com.google.cloud.datacatalog.lineage.v1.LineageSettings;

public interface GCPLineageTransportExtension {

  void updateSettings(LineageSettings.Builder builder);
}
