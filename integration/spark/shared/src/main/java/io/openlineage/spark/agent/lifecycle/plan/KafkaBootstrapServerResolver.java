/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Pattern;

public class KafkaBootstrapServerResolver {
  private static final Pattern KAFKA_PROTOCOL_PREFIX =
      Pattern.compile("^[A-Za-z][A-Za-z0-9_+.-]*://");

  static String resolve(Optional<String> bootstrapServersOpt) {
    String server =
        bootstrapServersOpt
            .map(str -> str.split(",")[0].trim())
            .map(KafkaBootstrapServerResolver::stripKafkaProtocol)
            .map(endpoint -> URI.create("kafka://" + endpoint))
            .map(uri -> uri.getHost() + ":" + uri.getPort())
            .orElse("");
    return "kafka://" + server;
  }

  private static String stripKafkaProtocol(String endpoint) {
    if (KAFKA_PROTOCOL_PREFIX.matcher(endpoint).find()) {
      return endpoint.substring(endpoint.indexOf("://") + 3);
    }
    return endpoint;
  }
}
