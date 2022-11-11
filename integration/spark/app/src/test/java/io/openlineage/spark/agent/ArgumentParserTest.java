/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.HttpConfig;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ArgumentParserTest {

  private static final String NS_NAME = "ns_name";
  private static final String JOB_NAME = "job_name";
  private static final String URL = "http://localhost:5000/api/v1/lineage";
  private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
  private static final String APP_NAME = "test";

  public static Collection<Object[]> data() {
    List<Object[]> pass = new ArrayList<>();
    pass.add(
        new Object[] {
          "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc",
          URL,
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
          URL + "?myParam=xyz",
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
          URL + "?myParam=xyz",
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
      String expectedUrl,
      String namespace,
      String jobName,
      String runId,
      boolean defaultRunId,
      Optional<String> apiKey,
      Optional<Double> timeout,
      Optional<String> appName,
      Optional<Map<String, String>> urlParams)
      throws URISyntaxException {
    ArgumentParser parser = ArgumentParser.parse(input);
    assertThat(parser.getTransportConfig()).isPresent().get().isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) parser.getTransportConfig().get();
    assertEquals(expectedUrl, httpConfig.getUrl().toString());
    assertEquals(namespace, parser.getNamespace());
    assertEquals(jobName, parser.getJobName());
    if (defaultRunId) {
      assertNull(parser.getParentRunId());
    } else {
      assertEquals(runId, parser.getParentRunId());
    }
    if (apiKey.isPresent()) {
      assertThat(httpConfig.getAuth()).isNotNull().isInstanceOf(ApiKeyTokenProvider.class);
    } else {
      assertThat(httpConfig.getAuth()).isNull();
    }
    assertThat(httpConfig.getTimeout()).isEqualTo(timeout.orElse(null));
    assertThat(parser.getAppName()).isEqualTo(appName);
    Optional<Map<String, String>> actualParams =
        Optional.ofNullable(httpConfig.getUrl().getQuery())
            .map(
                str ->
                    Stream.of(str.split("&"))
                        .map(
                            pair -> {
                              String[] split = pair.split("=");
                              assertThat(split.length).isEqualTo(2);
                              return Pair.of(split[0], split[1]);
                            })
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
    if (urlParams.isPresent()) {
      assertThat(actualParams).isPresent().get().isEqualTo(urlParams.get());
    } else {
      assertThat(actualParams).isNotPresent();
    }
  }
}
