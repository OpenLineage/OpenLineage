/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ArgumentParserTest {

  private static final String NS_NAME = "ns_name";
  private static final String JOB_NAME = "job_name";
  private static final String URL = "http://localhost:5000";
  private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";

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
          Optional.of(Collections.singletonMap("myParam", "xyz"))
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
      Optional<Map<String, String>> urlParams) {
    ArgumentParser parser = ArgumentParser.parse(input);
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
    assertEquals(urlParams, parser.getUrlParams());
    if (urlParams.isPresent()) {
      urlParams
          .get()
          .forEach((k, v) -> assertEquals(urlParams.get().get(k), parser.getUrlParam(k)));
    }
  }
}
