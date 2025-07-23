/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import java.net.URI;
import java.util.Optional;

public class KafkaBootstrapServerResolver {
  static URI resolve(Optional<String> bootstrapServersOpt) {
    return bootstrapServersOpt
        .map(
            str -> {
              if (!str.matches("\\w+://.*")) {
                return "PLAINTEXT://" + str;
              } else {
                return str;
              }
            })
        .map(str -> URI.create(str.split(",")[0]))
        .orElse(URI.create(""));
  }
}
