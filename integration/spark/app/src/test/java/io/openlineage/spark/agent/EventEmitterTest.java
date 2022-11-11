/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.client.transports.HttpConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EventEmitterTest {

  @Test
  void testLineageUri() throws URISyntaxException {
    ArgumentParser argumentParser =
        new ArgumentParser.ArgumentParserBuilder()
            .host("https://localhost:5000")
            .version("v1")
            .namespace("ns_name")
            .jobName("job_name")
            .parentRunId("ea445b5c-22eb-457a-8007-01c7c52b6e54")
            .apiKey(Optional.of("abc"))
            .build();
    HttpConfig config = EventEmitter.buildHttpConfig(argumentParser);
    assertEquals(URI.create("https://localhost:5000/api/v1/lineage"), config.getUrl());
  }

  @Test
  void testLineageUriWithExtraParams() throws URISyntaxException {
    HashMap<String, String> paramsMap = new HashMap<>();
    paramsMap.put("code", "123");
    paramsMap.put("foo", "bar");
    ArgumentParser argumentParser =
        new ArgumentParser.ArgumentParserBuilder()
            .host("https://localhost:5000")
            .version("v1")
            .namespace("ns_name")
            .jobName("job_name")
            .parentRunId("ea445b5c-22eb-457a-8007-01c7c52b6e54")
            .apiKey(Optional.of("abc"))
            .urlParams(Optional.of(paramsMap))
            .build();
    HttpConfig config = EventEmitter.buildHttpConfig(argumentParser);
    assertEquals(
        URI.create("https://localhost:5000/api/v1/lineage?code=123&foo=bar"), config.getUrl());
  }
}
