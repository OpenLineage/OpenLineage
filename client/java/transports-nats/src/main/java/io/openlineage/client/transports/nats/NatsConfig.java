/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.transports.TransportConfig;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
@Setter
public final class NatsConfig implements TransportConfig, MergeConfig<NatsConfig> {
  /** NATS server URL, or several comma-separated URLs. */
  private String url;

  private String subject;

  /** Publish through JetStream and wait for the stream's acknowledgement. Defaults to true. */
  private Boolean jetstream;

  /** Seconds to wait for the JetStream acknowledgement, or for the flush over core NATS. */
  private Double publishTimeout;

  private Double connectTimeout;

  /** Set the Nats-Msg-Id header for JetStream de-duplication. Defaults to true. */
  private Boolean msgIdHeader;

  /** Per-message TTL in seconds; the stream must allow per-message TTL. */
  private Integer messageTtl;

  private String user;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @ToString.Exclude
  private String password;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @ToString.Exclude
  private String token;

  /** Path to a file holding an NKey user seed. */
  private String nkeysSeed;

  /** Path to a .creds file holding a user JWT and NKey seed. */
  private String credsFile;

  private String tlsKeystorePath;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @ToString.Exclude
  private String tlsKeystorePassword;

  private String tlsTruststorePath;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @ToString.Exclude
  private String tlsTruststorePassword;

  /** Raw jnats options (io.nats.client.*); the settings above take precedence when set. */
  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @ToString.Exclude
  private Properties properties = new Properties();

  @Override
  public NatsConfig mergeWithNonNull(NatsConfig other) {
    Properties mergedProperties = new Properties();
    if (properties != null) {
      mergedProperties.putAll(properties);
    }
    if (other.properties != null) {
      mergedProperties.putAll(other.properties);
    }

    return new NatsConfig(
        mergePropertyWith(url, other.url),
        mergePropertyWith(subject, other.subject),
        mergePropertyWith(jetstream, other.jetstream),
        mergePropertyWith(publishTimeout, other.publishTimeout),
        mergePropertyWith(connectTimeout, other.connectTimeout),
        mergePropertyWith(msgIdHeader, other.msgIdHeader),
        mergePropertyWith(messageTtl, other.messageTtl),
        mergePropertyWith(user, other.user),
        mergePropertyWith(password, other.password),
        mergePropertyWith(token, other.token),
        mergePropertyWith(nkeysSeed, other.nkeysSeed),
        mergePropertyWith(credsFile, other.credsFile),
        mergePropertyWith(tlsKeystorePath, other.tlsKeystorePath),
        mergePropertyWith(tlsKeystorePassword, other.tlsKeystorePassword),
        mergePropertyWith(tlsTruststorePath, other.tlsTruststorePath),
        mergePropertyWith(tlsTruststorePassword, other.tlsTruststorePassword),
        mergedProperties);
  }
}
