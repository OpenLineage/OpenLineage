/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;

/** Bounded, thread-scoped diagnostics for existing dataset dispatch. */
@Slf4j
public final class DatasetDispatchTrace {
  private static final int RECORD_LIMIT = 1_000;
  private static final ThreadLocal<DatasetDispatchTrace> CURRENT = new ThreadLocal<>();

  private final String prefix;
  private final int limit;
  private final Consumer<String> sink;
  private final Map<Object, Node> nodes = new IdentityHashMap<>();
  private int records;
  private long nextInvocation;
  private long currentInvocation;

  private DatasetDispatchTrace(String label, int limit, Consumer<String> sink) {
    this.prefix = "capture=" + UUID.randomUUID() + " " + label;
    this.limit = limit;
    this.sink = sink;
  }

  /** Opens the scope on the thread actually performing extraction, not its waiting caller. */
  public static <T> T capture(
      UUID runId, SparkListenerEvent event, String phase, Supplier<T> work) {
    if (!log.isTraceEnabled()) {
      return work.get();
    }
    return capture(
        "run=" + runId + " event=" + typeName(event) + " phase=" + phase,
        RECORD_LIMIT,
        log::trace,
        work);
  }

  static <T> T capture(String label, int limit, Consumer<String> sink, Supplier<T> work) {
    DatasetDispatchTrace previous = CURRENT.get();
    DatasetDispatchTrace trace = new DatasetDispatchTrace(label, limit, sink);
    CURRENT.set(trace);
    try {
      return work.get();
    } finally {
      trace.nodes.clear();
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    }
  }

  static Invocation start(String operation, Object handler, Object target) {
    DatasetDispatchTrace trace = CURRENT.get();
    if (trace == null || !trace.hasCapacity()) {
      return Invocation.NOOP;
    }
    Node node = trace.node(target);
    long parent = trace.currentInvocation;
    long id = ++trace.nextInvocation;
    trace.currentInvocation = id;
    String handlerName = typeName(handler);
    if (handler instanceof QueryPlanVisitor) {
      try {
        handlerName = ((QueryPlanVisitor<?, ?>) handler).internalClassName();
      } catch (RuntimeException ignored) {
        // An extension's diagnostic name must not change extraction behaviour.
      }
    }
    trace.write(
        "id="
            + id
            + " parent="
            + parent
            + " operation="
            + operation
            + " handler="
            + handlerName
            + " node="
            + node.id
            + " nodeType="
            + typeName(target));
    return new Invocation(trace, id, parent);
  }

  /** Observes existing visited-state updates and rejections without changing their semantics. */
  public static void visited(Object target, String eventName, boolean added) {
    DatasetDispatchTrace trace = CURRENT.get();
    if (trace == null || !trace.hasCapacity()) {
      return;
    }
    Node node = trace.node(target);
    if (added) {
      node.visitedBy.put(eventName, trace.currentInvocation);
    }
    Long writer = node.visitedBy.get(eventName);
    trace.write(
        "id="
            + trace.currentInvocation
            + " node="
            + node.id
            + (added ? " visited=recorded" : " visited=already-visited")
            + " visitedBy="
            + (writer == null ? "outside-scope" : writer));
  }

  /** Mirrors an existing visited-state reset, leaving trace-local node identities intact. */
  public static void clearVisited() {
    DatasetDispatchTrace trace = CURRENT.get();
    if (trace != null) {
      trace.nodes.values().forEach(node -> node.visitedBy.clear());
    }
  }

  private Node node(Object target) {
    return nodes.computeIfAbsent(target, ignored -> new Node(nodes.size() + 1));
  }

  private boolean hasCapacity() {
    if (records < limit) {
      return true;
    }
    if (records == limit) {
      records++;
      emit("truncated=true limit=" + limit);
      nodes.clear();
    }
    return false;
  }

  private void write(String message) {
    if (hasCapacity()) {
      records++;
      emit(message);
    }
  }

  private void emit(String message) {
    try {
      sink.accept(prefix + " " + message);
    } catch (RuntimeException ignored) {
      // Diagnostics must not make a successful extraction fail.
    }
  }

  private static String typeName(Object value) {
    return value == null ? "null" : value.getClass().getName();
  }

  private static final class Node {
    private final int id;
    private final Map<String, Long> visitedBy = new HashMap<>();

    private Node(int id) {
      this.id = id;
    }
  }

  static final class Invocation implements AutoCloseable {
    private static final Invocation NOOP = new Invocation(null, 0, 0);
    private final DatasetDispatchTrace trace;
    private final long id;
    private final long parent;
    private final long started;
    private String outcome = "aborted=true";

    private Invocation(DatasetDispatchTrace trace, long id, long parent) {
      this.trace = trace;
      this.id = id;
      this.parent = parent;
      this.started = trace == null ? 0 : System.nanoTime();
    }

    void matched(boolean matched) {
      if (trace != null) {
        outcome = "match=" + matched;
      }
    }

    void returned(Collection<?> result) {
      if (trace != null) {
        try {
          outcome = result == null ? "result=null" : "resultCount=" + result.size();
        } catch (RuntimeException ignored) {
          outcome = "resultCount=unavailable";
        }
      }
    }

    void failed(Throwable error) {
      if (trace != null) {
        outcome = "error=" + error.getClass().getName();
      }
    }

    @Override
    public void close() {
      if (trace != null) {
        trace.currentInvocation = parent;
        trace.write("id=" + id + " " + outcome + " durationNanos=" + (System.nanoTime() - started));
      }
    }
  }
}
