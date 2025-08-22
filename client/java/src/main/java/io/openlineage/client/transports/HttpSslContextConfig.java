/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.MergeConfig;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class HttpSslContextConfig implements MergeConfig<HttpSslContextConfig> {
  @Getter @Setter private String storePassword = "";
  @Getter @Setter private String keyPassword = "";
  @Getter @Setter private @Nullable String keyStoreType;
  @Getter @Setter private String keyStorePath;

  @Override
  public HttpSslContextConfig mergeWithNonNull(HttpSslContextConfig other) {
    return new HttpSslContextConfig(
        mergePropertyWith(storePassword, other.storePassword),
        mergePropertyWith(keyPassword, other.keyPassword),
        mergePropertyWith(keyStoreType, other.keyStoreType),
        mergePropertyWith(keyStorePath, other.keyStorePath));
  }
}
