/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaBootstrapServerResolverTest {
  @Test
  void resolvesPlainBootstrapServer() {
    assertEquals(
        "kafka://broker.example.com:9092",
        KafkaBootstrapServerResolver.resolve(Optional.of("broker.example.com:9092")));
  }

  @Test
  void resolvesSecureBootstrapServerWithUnderscoreProtocol() {
    assertEquals(
        "kafka://broker.example.com:9093",
        KafkaBootstrapServerResolver.resolve(Optional.of("SASL_SSL://broker.example.com:9093")));
  }

  @Test
  void resolvesFirstBootstrapServer() {
    assertEquals(
        "kafka://broker-one.example.com:9092",
        KafkaBootstrapServerResolver.resolve(
            Optional.of("SASL_SSL://broker-one.example.com:9092,broker-two.example.com:9092")));
  }
}
