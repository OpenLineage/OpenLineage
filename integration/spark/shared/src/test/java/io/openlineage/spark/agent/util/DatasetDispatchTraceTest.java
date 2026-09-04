/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.DatasetDispatcherTest.handler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class DatasetDispatchTraceTest {
  @Test
  void recordsNestedDelegationWithItsActualParent() {
    List<String> trace = new ArrayList<>();
    PartialFunction<Object, List<String>> child =
        handler(
            "child", new ArrayList<>(), node -> true, node -> Collections.singletonList("dataset"));
    PartialFunction<Object, List<String>> parent =
        handler(
            "parent",
            new ArrayList<>(),
            node -> true,
            node -> new ArrayList<>(PlanUtils.merge(Collections.singletonList(child)).apply(node)));
    List<String> result =
        DatasetDispatchTrace.capture(
            "phase=input",
            100,
            trace::add,
            () ->
                DatasetDispatcher.collect("event", Collections.singletonList(parent))
                    .collect(Collectors.toList()));
    assertThat(result).containsExactly("dataset");
    assertThat(String.join("\n", trace))
        .contains(
            "id=2 parent=0 operation=apply",
            "id=3 parent=2 operation=check",
            "id=4 parent=2 operation=apply");
  }

  @Test
  void boundsOutputAndBookkeepingWithoutChangingExecution() throws Exception {
    List<String> trace = new ArrayList<>();
    List<String> calls = new ArrayList<>();
    PartialFunction<Object, List<String>> handler =
        handler("handler", calls, node -> true, node -> Collections.singletonList("result"));
    DatasetDispatchTrace.capture(
        "bounded",
        3,
        trace::add,
        () -> {
          for (int i = 0; i < 100; i++) {
            assertThat(DatasetDispatcher.apply(handler, new Object())).containsExactly("result");
          }
          assertThat(retainedNodes()).isEmpty();
          return null;
        });
    assertThat(calls).hasSize(100);
    assertThat(trace).hasSize(4);
    assertThat(trace.get(3)).contains("truncated=true limit=3");
  }

  @Test
  void scopeClosesAfterFailureAndRestoresOuterScope() {
    List<String> outer = new ArrayList<>();
    List<String> inner = new ArrayList<>();
    PartialFunction<Object, List<String>> handler =
        handler("handler", new ArrayList<>(), node -> true, node -> Collections.emptyList());
    DatasetDispatchTrace.capture(
        "outer",
        100,
        outer::add,
        () -> {
          assertThatThrownBy(
                  () ->
                      DatasetDispatchTrace.capture(
                          "inner",
                          100,
                          inner::add,
                          () -> {
                            DatasetDispatcher.apply(handler, "inner node");
                            throw new IllegalStateException("failure");
                          }))
              .isInstanceOf(IllegalStateException.class);
          DatasetDispatcher.apply(handler, "outer node");
          return null;
        });
    int size = outer.size();
    DatasetDispatcher.apply(handler, "outside");
    assertThat(outer).hasSize(size);
    assertThat(inner).hasSize(2);
    assertThat(String.join("\n", outer)).contains("outer").doesNotContain("inner");
    assertThat(currentTrace()).isNull();
  }

  @Test
  void scopesAreIsolatedOnWorkersAndDoNotLeakOnThreadReuse() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    List<String> first = new ArrayList<>();
    List<String> second = new ArrayList<>();
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (List<String> trace : Arrays.asList(first, second)) {
        futures.add(
            workers.submit(
                () ->
                    DatasetDispatchTrace.capture(
                        "worker",
                        100,
                        trace::add,
                        () -> {
                          entered.countDown();
                          await(release);
                          DatasetDispatcher.apply(
                              handler(
                                  "worker",
                                  new ArrayList<>(),
                                  node -> true,
                                  node -> Collections.emptyList()),
                              new Object());
                          return null;
                        })));
      }
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      release.countDown();
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
      assertThat(first).hasSize(2);
      assertThat(second).hasSize(2);
      assertThat(first.get(0).split(" ")[0]).isNotEqualTo(second.get(0).split(" ")[0]);
      assertThat(workers.submit(DatasetDispatchTraceTest::currentTrace).get(5, TimeUnit.SECONDS))
          .isNull();
      assertThat(workers.submit(DatasetDispatchTraceTest::currentTrace).get(5, TimeUnit.SECONDS))
          .isNull();
    } finally {
      release.countDown();
      workers.shutdownNow();
    }
  }

  @Test
  void doesNotHashRenderOrRetainNodesAfterScope() {
    Object node =
        new Object() {
          @Override
          public int hashCode() {
            throw new AssertionError("hashCode");
          }

          @Override
          public boolean equals(Object other) {
            throw new AssertionError("equals");
          }

          @Override
          public String toString() {
            throw new AssertionError("toString");
          }
        };
    List<String> trace = new ArrayList<>();
    Map<?, ?> retained =
        DatasetDispatchTrace.capture(
            "identity",
            100,
            trace::add,
            () -> {
              DatasetDispatcher.matches(
                  handler(
                      "handler",
                      new ArrayList<>(),
                      ignored -> true,
                      ignored -> Collections.emptyList()),
                  node);
              assertThat(retainedNodes()).hasSize(1);
              return retainedNodes();
            });
    assertThat(retained).isEmpty();
    assertThat(trace.get(0)).contains("node=1 nodeType=");
  }

  @Test
  void sinkFailureDoesNotChangeResults() {
    assertThat(
            DatasetDispatchTrace.capture(
                "sink-failure",
                100,
                ignored -> {
                  throw new IllegalStateException();
                },
                () ->
                    DatasetDispatcher.apply(
                        handler(
                            "handler",
                            new ArrayList<>(),
                            node -> true,
                            node -> Collections.singletonList("result")),
                        new Object())))
        .containsExactly("result");
    assertThat(currentTrace()).isNull();
  }

  @Test
  void traceDoesNotRenderDatasetValuesOrExceptionMessages() {
    List<String> trace = new ArrayList<>();
    DatasetDispatchTrace.capture(
        "privacy",
        100,
        trace::add,
        () -> {
          DatasetDispatcher.apply(
              handler(
                  "handler",
                  new ArrayList<>(),
                  node -> true,
                  node -> Collections.singletonList("private-dataset")),
              "private-node");
          DatasetDispatcher.matches(
              handler(
                  "failure",
                  new ArrayList<>(),
                  node -> {
                    throw new IllegalArgumentException("private-message");
                  },
                  node -> Collections.emptyList()),
              new Object());
          return null;
        });
    assertThat(String.join("\n", trace))
        .contains("resultCount=1", "error=java.lang.IllegalArgumentException")
        .doesNotContain("private-dataset", "private-node", "private-message");
  }

  @Test
  void visitedAttributionIsScopedByEventAndReset() {
    List<String> trace = new ArrayList<>();
    Object node = new Object();
    DatasetDispatchTrace.capture(
        "visited",
        100,
        trace::add,
        () -> {
          DatasetDispatcher.apply(
              handler(
                  "writer",
                  new ArrayList<>(),
                  ignored -> true,
                  ignored -> {
                    DatasetDispatchTrace.visited(node, "Start", true);
                    return Collections.emptyList();
                  }),
              node);
          DatasetDispatchTrace.visited(node, "Start", false);
          DatasetDispatchTrace.visited(node, "End", false);
          DatasetDispatchTrace.clearVisited();
          DatasetDispatchTrace.visited(node, "Start", false);
          return null;
        });
    List<String> rejections =
        trace.stream()
            .filter(line -> line.contains("visited=already-visited"))
            .collect(Collectors.toList());
    assertThat(rejections).hasSize(3);
    assertThat(rejections.get(0)).contains("visitedBy=1");
    assertThat(rejections.get(1)).contains("visitedBy=outside-scope");
    assertThat(rejections.get(2)).contains("visitedBy=outside-scope");
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Worker did not resume");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private static Object currentTrace() {
    try {
      Field field = DatasetDispatchTrace.class.getDeclaredField("CURRENT");
      field.setAccessible(true);
      return ((ThreadLocal<?>) field.get(null)).get();
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private static Map<?, ?> retainedNodes() {
    try {
      Field field = DatasetDispatchTrace.class.getDeclaredField("nodes");
      field.setAccessible(true);
      return (Map<?, ?>) field.get(currentTrace());
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
