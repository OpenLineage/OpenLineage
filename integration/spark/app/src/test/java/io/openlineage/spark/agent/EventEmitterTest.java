/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

class EventEmitterTest {

  ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();

  @Test
  void testLineageUri() throws URISyntaxException {
    SparkConf conf = new SparkConf()
            .set(ArgumentParser.SPARK_CONF_HTTP_URL, "https://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/"
            + "ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc")
            .set("spark.openlineage.transport.type", "http");
    ArgumentParser argPars = ArgumentParser.parse(conf);
    EventEmitter ctx = new EventEmitter(argPars);
//    ctx.getClient()
//    assertEquals(URI.create("https://localhost:5000/api/v1/lineage"), ctx.getLineageURI());
  }

  @Test
  void testLineageUriWithExtraParams() throws URISyntaxException {
    SparkConf conf = new SparkConf().set(ArgumentParser.SPARK_CONF_HTTP_URL, "https://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/"
            + "ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc&code=123&foo=bar")
            .set("spark.openlineage.transport.type", "http");
    ArgumentParser argPars = ArgumentParser.parse(conf);
    EventEmitter ctx = new EventEmitter(argPars);
//    assertEquals(
//        URI.create("https://localhost:5000/api/v1/lineage?code=123&foo=bar"), ctx.getLineageURI());
  }
}
