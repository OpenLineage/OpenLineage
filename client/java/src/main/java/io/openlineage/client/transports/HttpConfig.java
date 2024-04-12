/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.With;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@With
public final class HttpConfig implements TransportConfig {
  public enum Compression {
    @JsonProperty("gzip")
    GZIP
  };

  @Getter @Setter private URI url;
  @Getter @Setter private @Nullable String endpoint;

  @Getter @Setter
  private @Nullable Double
      timeout; // deprecated, will be removed in 1.13, assumes timeout is in seconds

  @Getter @Setter private @Nullable Integer timeoutInMillis;
  @Getter @Setter private @Nullable TokenProvider auth;
  @Getter @Setter private @Nullable Map<String, String> urlParams;
  @Getter @Setter private @Nullable Map<String, String> headers;
  @Getter @Setter private @Nullable Compression compression;
}
