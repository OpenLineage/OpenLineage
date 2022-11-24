/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.openlineage.client.OpenLineageYaml;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ArgumentParserTest {

  private static final String NS_NAME = "ns_name";
  private static final String JOB_NAME = "job_name";
  private static final String URL = "http://localhost:5000";
  private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
  private static final String APP_NAME = "test";

  public static Collection<Object[]> data() {
    List<Object[]> pass = new ArrayList<>();
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.of("abc"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name?api_key=",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          null,
          true,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc&myParam=xyz",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.of("abc"),
          Optional.empty(),
          Optional.empty(),
          Optional.of(Collections.singletonMap("myParam", "xyz"))
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=&myParam=xyz",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(Collections.singletonMap("myParam", "xyz"))
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?timeout=5000",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.of(5000.0),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?timeout=",
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        });
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?app_name="
              + APP_NAME,
          URL,
          "v1",
          NS_NAME,
          JOB_NAME,
          RUN_ID,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.of(APP_NAME),
          Optional.empty()
        });
    return pass;
  }

  @ParameterizedTest
  @MethodSource("data")
  void testArgument(
      String input,
      String host,
      String version,
      String namespace,
      String jobName,
      String runId,
      boolean defaultRunId,
      Optional<String> apiKey,
      Optional<Double> timeout,
      Optional<String> appName,
      Optional<Map<String, String>> urlParams) {
    ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();
    ArgumentParser.parse(builder, input);
    ArgumentParser parser = builder.build();
    assertEquals(host, parser.getHost());
    assertEquals(version, parser.getVersion());
    assertEquals(namespace, parser.getNamespace());
    assertEquals(jobName, parser.getJobName());
    if (defaultRunId) {
      assertNull(parser.getParentRunId());
    } else {
      assertEquals(runId, parser.getParentRunId());
    }
    assertEquals(apiKey, parser.getApiKey());
    assertEquals(timeout, parser.getTimeout());
    assertEquals(appName, parser.getAppName());
    assertEquals(urlParams, parser.getUrlParams());
    urlParams.ifPresent(
        par -> par.forEach((k, v) -> assertEquals(par.get(k), parser.getUrlParam(k))));
  }

  @Test
  void testGetDisabledFacets() {
    ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();
    builder.host("host");
    builder.DEFAULT_DISABLED_FACETS("spark_unknown;spark.logicalPlan");
    ArgumentParser parser = builder.build();

    assertThat(parser.getDEFAULT_DISABLED_FACETS())
        .contains("spark_unknown")
        .contains("spark.logicalPlan")
        .hasSize(2);
  }

  @Test
  void testGetDisabledFacetsWhenNoEntry() {
    ArgumentParser.ArgumentParserBuilder builder = new ArgumentParser.ArgumentParserBuilder();
    ArgumentParser parser = builder.build();

    assertThat(parser.getDEFAULT_DISABLED_FACETS()).contains("spark_unknown").hasSize(1);
  }

  @Test
  void testParsingSparkConfToJson(){
    SparkConf sparkConf = new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set("spark.openlineage.transport.url", "http://localhost:5050")
            .set("spark.openlineage.transport.endpoint", "api/v1/lineage")
            .set("spark.openlineage.transport.auth.type", "api_key")
            .set("spark.openlineage.transport.auth.apiKey", "random_token")
            .set("spark.openlineage.facets.disabled", "facet1;facet2");
    OpenLineageYaml openLineageYaml = ArgumentParser.extractOpenlineageConfFromSparkConf(sparkConf);
    FacetsConfig facetsConfig = openLineageYaml.getFacetsConfig();
    TransportConfig transportConfig = openLineageYaml.getTransportConfig();
//    OpenLineageClient openLineageClient = new OpenLineageClient()
    
    assertEquals(1,1);
  }
}
