/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.agent.util.DatasetDispatchTrace.Invocation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;

/** Executes existing dataset handlers without changing their order or selection semantics. */
@Slf4j(topic = "io.openlineage.spark.agent.util.PlanUtils")
public final class DatasetDispatcher {
  private DatasetDispatcher() {}

  /** Combines sequential all-match application with short-circuit applicability checks. */
  public static <T, D> OpenLineageAbstractPartialFunction<T, Collection<D>> merge(
      Collection<? extends PartialFunction<T, ? extends Collection<D>>> handlers) {
    return new OpenLineageAbstractPartialFunction<T, Collection<D>>() {
      private String appliedClassName;

      @Override
      public boolean isDefinedAt(T node) {
        return handlers.stream().anyMatch(handler -> matches(handler, node));
      }

      @Override
      public Collection<D> apply(T node) {
        // Do not precompute matches: applying a handler may change later eligibility.
        return handlers.stream()
            .filter(handler -> matches(handler, node))
            .map(
                handler -> {
                  try (Invocation invocation = DatasetDispatchTrace.start("apply", handler, node)) {
                    try {
                      Collection<D> result = handler.apply(node);
                      if (log.isDebugEnabled()) {
                        log.debug(
                            "Visitor {} visited {}, returned {}",
                            handler.getClass().getCanonicalName(),
                            node.getClass().getCanonicalName(),
                            result);
                      }
                      appliedClassName = node.getClass().getName();
                      invocation.returned(result);
                      return result;
                    } catch (RuntimeException | NoClassDefFoundError | NoSuchMethodError e) {
                      invocation.failed(e);
                      log.error("Apply failed:", e);
                      return null;
                    }
                  }
                })
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      }

      @Override
      String appliedName() {
        return appliedClassName;
      }
    };
  }

  /** Preserves the event/node builder path's lazy check, safe application, and flattening. */
  public static <T, D> Stream<D> collect(
      T node, Collection<? extends PartialFunction<T, List<D>>> handlers) {
    return handlers.stream()
        .filter(handler -> matches(handler, node))
        .map(handler -> apply(handler, node))
        .flatMap(Collection::stream);
  }

  /** Checks applicability with the same recovery rules as PlanUtils.safeIsDefinedAt. */
  public static boolean matches(PartialFunction handler, Object node) {
    try (Invocation invocation = DatasetDispatchTrace.start("check", handler, node)) {
      try {
        boolean matched = handler.isDefinedAt(node);
        invocation.matched(matched);
        return matched;
      } catch (ClassCastException e) {
        invocation.failed(e);
        return false;
      } catch (TypeNotPresentException e) {
        invocation.failed(e);
        log.info("isDefinedAt method failed due to missing type: {}", e.getMessage());
        return false;
      } catch (Exception e) {
        invocation.failed(e);
        log.info("isDefinedAt method failed on {}", e);
        return false;
      } catch (NoClassDefFoundError e) {
        invocation.failed(e);
        log.info("isDefinedAt method failed on {}", e.getMessage());
        return false;
      }
    }
  }

  /**
   * Applies a standalone builder with its existing recovery rules. Unlike merged application, this
   * catches checked exceptions and returns an empty list on failure; null is passed through.
   */
  public static <T, D> List<D> apply(PartialFunction<T, List<D>> handler, T node) {
    try (Invocation invocation = DatasetDispatchTrace.start("apply", handler, node)) {
      try {
        List<D> result = handler.apply(node);
        invocation.returned(result);
        return result;
      } catch (Exception | NoClassDefFoundError | NoSuchMethodError e) {
        invocation.failed(e);
        log.info("apply method failed with", e);
        return Collections.emptyList();
      }
    }
  }
}
