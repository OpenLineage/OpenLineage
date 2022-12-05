/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
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
  @Getter @Setter private URI url;
  @Getter @Setter private @Nullable String endpoint;
  @Getter @Setter private @Nullable Double timeout;
  @Getter @Setter private @Nullable TokenProvider auth;
  @Getter @Setter private @Nullable Map<String, String> urlParams;
}
