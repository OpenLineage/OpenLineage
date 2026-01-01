/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.jsonunit.core.Configuration;
import net.javacrumbs.jsonunit.core.Option;
import net.javacrumbs.jsonunit.core.internal.Diff;
import net.javacrumbs.jsonunit.core.internal.Options;
import net.javacrumbs.jsonunit.core.listener.Difference;
import net.javacrumbs.jsonunit.core.listener.DifferenceContext;
import net.javacrumbs.jsonunit.core.listener.DifferenceListener;
import org.apache.commons.lang.StringUtils;

/**
 * Utility class to verify if expected JSON content of the OpenLineage event is present among the
 * events emitted.
 */
@Slf4j
public class RunEventVerifier {

  private final List<String> events;

  @SuppressWarnings("PMD.AvoidStringBufferField")
  private StringBuilder diffDescription = new StringBuilder();

  @SneakyThrows
  private RunEventVerifier(List<String> events) {
    this.events = events;
  }

  /**
   * Path to a file containing in each line a separate JSON OpenLineage event.
   *
   * @param eventsFile
   * @return
   */
  public static RunEventVerifier of(File eventsFile) {
    List<String> lines;
    try {
      lines = Files.readAllLines(eventsFile.toPath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to read events files", e);
    }
    return new RunEventVerifier(lines);
  }

  @SneakyThrows
  public static RunEventVerifier ofStrings(List<String> events) {
    return new RunEventVerifier(events);
  }

  @SneakyThrows
  public static RunEventVerifier ofRunEvents(List<RunEvent> events) {
    return new RunEventVerifier(
        events.stream().map(e -> OpenLineageClientUtils.toJson(e)).collect(Collectors.toList()));
  }

  @SneakyThrows
  public boolean match(File... expectationFiles) {
    boolean matchesAll = true;

    for (File expectationFile : expectationFiles) {
      String expectedJson =
          new String(Files.readAllBytes(expectationFile.toPath()), StandardCharsets.UTF_8);
      JsonStringMatcher matcher = new JsonStringMatcher(expectedJson);

      // each event has to be valid for some of the events contained
      boolean matches = events.stream().anyMatch(e -> matcher.matches(e));

      if (!matches) {
        diffDescription
            .append("Couldn't match event for ")
            .append(expectationFile.getName())
            .append(System.lineSeparator())
            .append(bestEffortDiffDescription(events, expectedJson))
            .append(System.lineSeparator());
      }

      matchesAll = matchesAll && matches;
    }

    return matchesAll;
  }

  public String getDiffDescription() {
    return diffDescription.toString();
  }

  /**
   * Given a list of emitted events and an event that was expected to be contained within in, the
   * method tries to find the closest event to the expected.
   *
   * <p>The heuristics implemented is: * find the events with the same eventType * find the name of
   * the output dataset within expected event or input dataset name if output is not present * find
   * the event with the closest Levenstein distance within dataset name * print diff from
   * JsonStringMatcher
   *
   * @param events
   * @param expectedEvent
   */
  private String bestEffortDiffDescription(List<String> events, String expectedEvent) {
    RunEvent expectedRunEvent = OpenLineageClientUtils.runEventFromJson(expectedEvent);
    List<RunEvent> emitted =
        events.stream()
            .map(e -> OpenLineageClientUtils.runEventFromJson(e))
            .filter(e -> e.getEventType().equals(expectedRunEvent.getEventType()))
            .collect(Collectors.toList());
    RunEvent closest;

    if (!hasOutputDatasetName(expectedRunEvent) && !hasInputDatasetName(expectedRunEvent)) {
      log.warn("Cannot print best effort diff when no output nor input datasets in expected event");
      return "";
    }

    if (hasInputDatasetName(expectedRunEvent)) {
      Map<Integer, RunEvent> eventMap =
          emitted.stream()
              .filter(this::hasInputDatasetName)
              .collect(
                  Collectors.toMap(
                      e ->
                          StringUtils.getLevenshteinDistance(
                              e.getInputs().get(0).getName(),
                              expectedRunEvent.getInputs().get(0).getName()),
                      e -> e,
                      (e1, e2) -> e1 // this should not happen for closest event
                      ));
      closest = eventMap.get(Collections.min(eventMap.keySet()));
    } else {
      Map<Integer, RunEvent> eventMap =
          emitted.stream()
              .filter(this::hasOutputDatasetName)
              .collect(
                  Collectors.toMap(
                      e ->
                          StringUtils.getLevenshteinDistance(
                              e.getOutputs().get(0).getName(),
                              expectedRunEvent.getOutputs().get(0).getName()),
                      e -> e,
                      (e1, e2) -> e1 // this should not happen for closest event
                      ));
      closest = eventMap.get(Collections.min(eventMap.keySet()));
    }

    JsonStringMatcher matcher = new JsonStringMatcher(expectedEvent);
    return "Best effort events difference is: "
        + matcher.diff(OpenLineageClientUtils.toJson(closest)).differences();
  }

  private boolean hasInputDatasetName(RunEvent event) {
    return Optional.ofNullable(event.getInputs())
        .filter(l -> !l.isEmpty())
        .map(l -> l.get(0))
        .map(i -> i.getName())
        .isPresent();
  }

  private boolean hasOutputDatasetName(RunEvent event) {
    return Optional.ofNullable(event.getOutputs())
        .filter(l -> !l.isEmpty())
        .map(l -> l.get(0))
        .map(i -> i.getName())
        .isPresent();
  }

  /**
   * The <a
   * href="https://github.com/mock-server/mockserver/blob/mockserver-5.15.0/mockserver-core/src/main/java/org/mockserver/matchers/JsonStringMatcher.java">JsonStringMatcher</a>
   * class is a modified version of the JsonStringMatcher class within mock-server project. The main
   * reason for doing so is the lack of public constructor within the original version.
   */
  @Slf4j
  public static class JsonStringMatcher {
    private final ObjectMapper MAPPER;
    private final String matcher;
    private JsonNode matcherJsonNode;

    JsonStringMatcher(String matcher) {
      MAPPER = new ObjectMapper();
      this.matcher = matcher;
    }

    public boolean matches(String matched) {
      return diff(matched).similar();
    }

    public Diff diff(String matched) {
      try {
        if (StringUtils.isNotBlank(this.matcher)) {
          Options options = Options.empty();
          options =
              options.with(
                  Option.IGNORING_ARRAY_ORDER,
                  new Option[] {Option.IGNORING_EXTRA_ARRAY_ITEMS, Option.IGNORING_EXTRA_FIELDS});
          DifferenceResolver diffListener = new DifferenceResolver(MAPPER);
          Configuration diffConfig =
              Configuration.empty().withDifferenceListener(diffListener).withOptions(options);

          try {
            if (this.matcherJsonNode == null) {
              this.matcherJsonNode = MAPPER.readTree(matcher);
            }

            return Diff.create(this.matcherJsonNode, MAPPER.readTree(matched), "", "", diffConfig);
          } catch (Exception e) {
            log.error(
                "exception while perform json match failed expected:{}found:{}",
                new Object[] {this.matcher, matched});
          }
        }
      } catch (Exception e) {
        log.error(
            "json match failed expected:{}found:{}failed because:{}",
            new Object[] {this.matcher, matched, e.getMessage()});
      }

      throw new AssertionError("Unable to create Json diff.");
    }

    private static class DifferenceResolver implements DifferenceListener {
      public List<String> differences;
      private ObjectMapper MAPPER;

      DifferenceResolver(ObjectMapper MAPPER) {
        this.differences = new ArrayList();
        this.MAPPER = MAPPER;
      }

      @Override
      public void diff(Difference difference, DifferenceContext context) {
        switch (difference.getType()) {
          case EXTRA:
            this.differences.add(
                "additional element at \""
                    + difference.getActualPath()
                    + "\" with value: "
                    + this.prettyPrint(difference.getActual()));
            break;
          case MISSING:
            this.differences.add("missing element at \"" + difference.getActualPath() + "\"");
            break;
          case DIFFERENT:
            this.differences.add(
                "wrong value at \""
                    + difference.getActualPath()
                    + "\", expected: "
                    + this.prettyPrint(difference.getExpected())
                    + " but was: "
                    + this.prettyPrint(difference.getActual()));
            break;
        }
      }

      private String prettyPrint(Object value) {
        try {
          return this.MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
          return String.valueOf(value);
        }
      }
    }
  }
}
