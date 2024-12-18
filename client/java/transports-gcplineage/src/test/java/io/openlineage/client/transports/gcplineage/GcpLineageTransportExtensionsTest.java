/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.datalineage.producerclient.v1.AsyncLineageProducerClientSettings;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcpLineageTransportExtensionsTest {

  @BeforeEach
  void setUp() {
    TestGCPLineageTransportExtension.updateSettingsCalled = false;
  }

  @Test
  void testCreateSettingsCallsExtensions() throws IOException {
    GcpLineageTransportConfig config = new GcpLineageTransportConfig();
    AsyncLineageProducerClientSettings.Builder builder =
        AsyncLineageProducerClientSettings.newBuilder();

    GcpLineageTransport.ProducerClientWrapper.createSettings(config, builder);

    assertTrue(
        TestGCPLineageTransportExtension.updateSettingsCalled,
        "Extension's updateSettings method was not called");
  }
}
