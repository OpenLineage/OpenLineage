/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class HttpConfig implements TransportConfig, MergeConfig<HttpConfig> {
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

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @Getter
  @Setter
  private @Nullable TokenProvider auth;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @Getter
  @Setter
  private @Nullable Map<String, String> urlParams;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @Getter
  @Setter
  private @Nullable Map<String, String> headers;

  @Getter @Setter private @Nullable Compression compression;

  @Override
  public HttpConfig mergeWithNonNull(HttpConfig other) {
    return new HttpConfig(
        mergePropertyWith(url, other.url),
        mergePropertyWith(endpoint, other.endpoint),
        mergePropertyWith(timeout, other.timeout),
        mergePropertyWith(timeoutInMillis, other.timeoutInMillis),
        mergePropertyWith(auth, other.auth),
        mergePropertyWith(urlParams, other.urlParams),
        mergePropertyWith(headers, other.headers),
        mergePropertyWith(compression, other.compression));
  }
}
