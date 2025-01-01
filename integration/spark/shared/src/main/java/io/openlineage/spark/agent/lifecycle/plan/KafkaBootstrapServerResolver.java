/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import java.net.URI;
import java.util.Optional;

public class KafkaBootstrapServerResolver {
  static String resolve(Optional<String> bootstrapServersOpt) {
    String server =
        bootstrapServersOpt
            .map(
                str -> {
                  if (!str.matches("\\w+://.*")) {
                    return "PLAINTEXT://" + str;
                  } else {
                    return str;
                  }
                })
            .map(str -> URI.create(str.split(",")[0]))
            .map(uri -> uri.getHost() + ":" + uri.getPort())
            .orElse("");
    return "kafka://" + server;
  }
}
