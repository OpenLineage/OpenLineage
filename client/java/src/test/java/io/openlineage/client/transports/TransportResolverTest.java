/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;

class TransportResolverTest {

  @Test
  void testResolveTransportConfigByTypeForCommonType() {
    assertThat(TransportResolver.resolveTransportConfigByType("http")).isSameAs(HttpConfig.class);
  }

  @Test
  void testResolveTransportByConfigForCommonType() {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    assertThat(TransportResolver.resolveTransportByConfig(config))
        .isInstanceOf(HttpTransport.class);
  }
}
