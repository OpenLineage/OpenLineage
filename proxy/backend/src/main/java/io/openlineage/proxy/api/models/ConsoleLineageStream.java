/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy.api.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** ConsoleLineageStream pushes events to stdout */
@Slf4j
public class ConsoleLineageStream extends LineageStream {
  public ConsoleLineageStream() {
    super(Type.CONSOLE);
  }

  @Override
  public void collect(@NonNull String eventAsString) {
    eventAsString = eventAsString.trim();
    if (eventAsString.startsWith("{") && eventAsString.endsWith("}")) {
      // assume the payload is int JSON, and perform json formatting.
      ObjectMapper mapper = new ObjectMapper();
      try {
        Object jsonObject = mapper.readValue(eventAsString, Object.class);
        String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        log.info(prettyJson);
      } catch (JsonProcessingException jpe) {
        log.info(eventAsString);
      }
    } else {
      log.info(eventAsString);
    }
  }
}
