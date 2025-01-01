/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public final class OpenLineageHttpHandler implements HttpHandler {
  private final List<String> events = new ArrayList<>();
  private final Map<String, List<RunEvent>> eventsMap = new HashMap<>();

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    InputStreamReader isr =
        new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
    BufferedReader br = new BufferedReader(isr);
    String value = br.readLine();

    events.add(value);

    RunEvent runEvent = OpenLineageClientUtils.runEventFromJson(value);
    String jobName = runEvent.getJob().getName();

    Optional<String> jobNameShort = Arrays.stream(jobName.split("\\.")).findFirst();

    if (!jobNameShort.isPresent()) {
      return;
    }

    String jobNameShortString = jobNameShort.get();

    if (!eventsMap.containsKey(jobNameShortString)) {
      eventsMap.put(jobNameShortString, new ArrayList<>());
    }

    eventsMap.get(jobNameShortString).add(runEvent);

    exchange.sendResponseHeaders(200, 0);
    try (Writer writer =
        new OutputStreamWriter(exchange.getResponseBody(), StandardCharsets.UTF_8)) {
      writer.write("{}");
    }
  }

  public void clear() {
    events.clear();
    eventsMap.clear();
  }
}
