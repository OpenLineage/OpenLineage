/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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

/**
 * Explicit proxy configuration for the HTTP transport.
 *
 * <p>When this is set the transport will route all outgoing requests through the specified proxy.
 * Alternatively, JVM system properties ({@code -Dhttps.proxyHost} / {@code -Dhttps.proxyPort}, or
 * their {@code http.*} equivalents) can be used without any config change; explicit config takes
 * precedence.
 *
 * <pre>{@code
 * transport:
 *   type: http
 *   url: https://lineage-endpoint
 *   proxy:
 *     host: squid.internal
 *     port: 3128
 * }</pre>
 */
@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class HttpProxyConfig implements MergeConfig<HttpProxyConfig> {
  /** Proxy host name or IP address. */
  @Getter @Setter private @Nullable String host;

  /** Proxy port. Defaults to 8080 when unset. */
  @Getter @Setter private @Nullable Integer port;

  /** Comma-separated list of hosts that bypass the proxy (passed to the route planner). */
  @Getter @Setter private @Nullable String nonProxyHosts;

  @Override
  public HttpProxyConfig mergeWithNonNull(HttpProxyConfig other) {
    return new HttpProxyConfig(
        mergePropertyWith(host, other.host),
        mergePropertyWith(port, other.port),
        mergePropertyWith(nonProxyHosts, other.nonProxyHosts));
  }
}
