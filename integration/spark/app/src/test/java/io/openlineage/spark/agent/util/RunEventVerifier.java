/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
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
import org.apache.commons.lang.StringUtils;
import org.mockserver.serialization.ObjectMapperFactory;
import shaded_package.com.fasterxml.jackson.core.JsonProcessingException;
import shaded_package.com.fasterxml.jackson.databind.JsonNode;
import shaded_package.com.fasterxml.jackson.databind.JsonSerializer;
import shaded_package.com.fasterxml.jackson.databind.ObjectWriter;
import shaded_package.net.javacrumbs.jsonunit.core.Configuration;
import shaded_package.net.javacrumbs.jsonunit.core.Option;
import shaded_package.net.javacrumbs.jsonunit.core.internal.Diff;
import shaded_package.net.javacrumbs.jsonunit.core.internal.Options;
import shaded_package.net.javacrumbs.jsonunit.core.listener.DifferenceContext;
import shaded_package.net.javacrumbs.jsonunit.core.listener.DifferenceListener;

/**
 * Utility class to verify if expected JSON content of the OpenLineage event is present among the
 * events emitted.
 */
@Slf4j
public class RunEventVerifier {

  private final List<String> events;
  private String diffDescription;

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
  @SneakyThrows
  public static RunEventVerifier of(File eventsFile) {
    return new RunEventVerifier(Files.readAllLines(eventsFile.toPath()));
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
      boolean matches = events.stream().filter(e -> matcher.matches(e)).findAny().isPresent();

      if (!matches) {
        diffDescription =
            new StringBuilder()
                .append("Couldn't match event for ")
                .append(expectationFile.getName())
                .append(System.lineSeparator())
                .append(bestEffortDiffDescription(events, expectedJson))
                .append(System.lineSeparator())
                .toString();
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
    private static final ObjectWriter PRETTY_PRINTER =
        ObjectMapperFactory.createObjectMapper(true, false, new JsonSerializer[0]);
    private final String matcher;
    private JsonNode matcherJsonNode;

    JsonStringMatcher(String matcher) {
      this.matcher = matcher;
    }

    public boolean matches(String matched) {
      return diff(matched).similar();
    }

    public Diff diff(String matched) {
      try {
        if (shaded_package.org.apache.commons.lang3.StringUtils.isNotBlank(this.matcher)) {
          Options options = Options.empty();
          options =
              options.with(
                  Option.IGNORING_ARRAY_ORDER,
                  new Option[] {Option.IGNORING_EXTRA_ARRAY_ITEMS, Option.IGNORING_EXTRA_FIELDS});
          JsonStringMatcher.Difference diffListener = new JsonStringMatcher.Difference();
          Configuration diffConfig =
              Configuration.empty().withDifferenceListener(diffListener).withOptions(options);

          try {
            if (this.matcherJsonNode == null) {
              this.matcherJsonNode =
                  ObjectMapperFactory.createObjectMapper().readTree(this.matcher);
            }

            return Diff.create(
                this.matcherJsonNode,
                ObjectMapperFactory.createObjectMapper().readTree(matched),
                "",
                "",
                diffConfig);
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

    private static class Difference implements DifferenceListener {
      public List<String> differences;

      Difference() {
        this.differences = new ArrayList();
      }

      @Override
      public void diff(
          shaded_package.net.javacrumbs.jsonunit.core.listener.Difference difference,
          DifferenceContext context) {
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
          return JsonStringMatcher.PRETTY_PRINTER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
          return String.valueOf(value);
        }
      }
    }
  }
}
