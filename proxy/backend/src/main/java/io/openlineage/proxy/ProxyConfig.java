/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** ProxyConfig defines the structure of the configuration file proxy.yml */
@NoArgsConstructor
public final class ProxyConfig extends Configuration {
  @Getter
  @JsonProperty("proxy")
  private final ProxyStreamFactory proxyStreamFactory = new ProxyStreamFactory();
}
