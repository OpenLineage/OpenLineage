/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

class EventEmitterTest {

  ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();

  @Test
  void testLineageUri() throws URISyntaxException {
    ArgumentParser.parseUrl(
        builder,
        "https://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/"
            + "ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc");
    EventEmitter ctx = new EventEmitter(builder.build());
    assertEquals(URI.create("https://localhost:5000/api/v1/lineage"), ctx.getLineageURI());
  }

  @Test
  void testLineageUriWithExtraParams() throws URISyntaxException {
    ArgumentParser.parseUrl(
        builder,
        "https://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/"
            + "ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc&code=123&foo=bar");
    EventEmitter ctx = new EventEmitter(builder.build());
    assertEquals(
        URI.create("https://localhost:5000/api/v1/lineage?code=123&foo=bar"), ctx.getLineageURI());
  }
}
