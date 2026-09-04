/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.VisitedNodes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

class DatasetDispatcherTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesShortCircuitChecksAndSequentialApplications(boolean traced) {
    List<String> calls = new ArrayList<>();
    PartialFunction<Object, List<String>> first =
        handler("first", calls, node -> true, node -> Arrays.asList("a", "b"));
    PartialFunction<Object, List<String>> second =
        handler("second", calls, node -> true, node -> Collections.singletonList("c"));
    OpenLineageAbstractPartialFunction<Object, Collection<String>> merged =
        PlanUtils.merge(Arrays.asList(first, second));
    List<String> trace = new ArrayList<>();
    Runnable extraction =
        () -> {
          assertThat(merged.isDefinedAt("node")).isTrue();
          assertThat(merged.apply("node")).containsExactly("a", "b", "c");
          assertThat(merged.appliedName()).isEqualTo(String.class.getName());
        };
    if (traced) {
      DatasetDispatchTrace.capture(
          "test",
          100,
          trace::add,
          () -> {
            extraction.run();
            return null;
          });
    } else {
      extraction.run();
    }
    assertThat(calls)
        .containsExactly(
            "first.check", "first.check", "first.apply", "second.check", "second.apply");
    assertThat(trace.isEmpty()).isEqualTo(!traced);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void doesNotPrecomputeMatches(boolean merged) {
    AtomicBoolean available = new AtomicBoolean(true);
    List<String> calls = new ArrayList<>();
    PartialFunction<Object, List<String>> first =
        handler(
            "first",
            calls,
            node -> true,
            node -> {
              available.set(false);
              return Collections.singletonList("first");
            });
    PartialFunction<Object, List<String>> second =
        handler(
            "second", calls, node -> available.get(), node -> Collections.singletonList("second"));
    Collection<String> result =
        merged
            ? PlanUtils.merge(Arrays.asList(first, second)).apply("node")
            : DatasetDispatcher.collect("node", Arrays.asList(first, second))
                .collect(Collectors.toList());
    assertThat(result).containsExactly("first");
    assertThat(calls).containsExactly("first.check", "first.apply", "second.check");
  }

  @Test
  void preservesNullEmptyAndExceptionRecoveryInMergedApplication() {
    List<String> calls = new ArrayList<>();
    List<String> trace = new ArrayList<>();
    List<PartialFunction<Object, List<String>>> handlers =
        Arrays.asList(
            handler("unmatched", calls, node -> false, node -> Collections.singletonList("unused")),
            handler("null", calls, node -> true, node -> null),
            handler("empty", calls, node -> true, node -> Collections.emptyList()),
            handler(
                "failed",
                calls,
                node -> true,
                node -> {
                  throw new NoSuchMethodError("private message");
                }),
            handler("valid", calls, node -> true, node -> Collections.singletonList("value")));
    Collection<String> results =
        DatasetDispatchTrace.capture(
            "test", 100, trace::add, () -> PlanUtils.merge(handlers).apply("node"));
    assertThat(results).containsExactly("value");
    assertThat(calls).doesNotContain("unmatched.apply");
    assertThat(String.join("\n", trace))
        .contains(
            "match=false",
            "result=null",
            "resultCount=0",
            "error=java.lang.NoSuchMethodError",
            "resultCount=1")
        .doesNotContain("private message");
  }

  @Test
  void preservesStandaloneCheckedExceptionAndNullBehaviour() {
    PartialFunction<Object, List<String>> checked =
        new AbstractPartialFunction<Object, List<String>>() {
          @Override
          public boolean isDefinedAt(Object node) {
            return true;
          }

          @Override
          @SneakyThrows
          public List<String> apply(Object node) {
            throw new IOException("failure");
          }
        };
    assertThat(PlanUtils.safeApply(checked, "node")).isEmpty();
    // Merged application historically catches RuntimeException, not arbitrary checked exceptions.
    assertThatThrownBy(() -> PlanUtils.merge(Collections.singletonList(checked)).apply("node"))
        .isInstanceOf(IOException.class);
    PartialFunction<Object, List<String>> returnsNull =
        handler("null", new ArrayList<>(), node -> true, node -> null);
    assertThat(PlanUtils.safeApply(returnsNull, "node")).isNull();
    assertThatThrownBy(
            () ->
                DatasetDispatcher.collect("node", Collections.singletonList(returnsNull))
                    .collect(Collectors.toList()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void preservesPredicateRecoveryAndPropagatesUncaughtErrors() {
    List<String> calls = new ArrayList<>();
    for (Throwable error :
        Arrays.asList(
            new ClassCastException(),
            new TypeNotPresentException("type", null),
            new IllegalStateException(),
            new NoClassDefFoundError())) {
      PartialFunction<Object, List<String>> failing =
          handler("failure", calls, node -> throwUnchecked(error), node -> Collections.emptyList());
      assertThat(PlanUtils.safeIsDefinedAt(failing, "node")).isFalse();
    }
    PartialFunction<Object, List<String>> fatal =
        handler(
            "fatal",
            calls,
            node -> {
              throw new AssertionError();
            },
            node -> Collections.emptyList());
    assertThatThrownBy(() -> PlanUtils.safeIsDefinedAt(fatal, "node"))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void directBuilderCollectionIsLazyAndOrdered() {
    List<String> calls = new ArrayList<>();
    PartialFunction<Object, List<String>> builder =
        handler("builder", calls, node -> true, node -> Collections.singletonList("dataset"));
    java.util.stream.Stream<String> results =
        DatasetDispatcher.collect("event", Collections.singletonList(builder));
    assertThat(calls).isEmpty();
    assertThat(results.collect(Collectors.toList())).containsExactly("dataset");
    assertThat(calls).containsExactly("builder.check", "builder.apply");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesVisitedSuppressionAndReportsUnderlyingBuilder(boolean emptyFirst) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getVisitedNodes()).thenReturn(new VisitedNodes());
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    SparkListenerEvent event = mock(SparkListenerEvent.class);
    LogicalPlan node = mock(LogicalPlan.class);
    InputDataset dataset = mock(InputDataset.class);
    RecordingBuilder first =
        new RecordingBuilder(
            context, emptyFirst ? Collections.emptyList() : Collections.singletonList(dataset));
    RecordingBuilder second = new RecordingBuilder(context, Collections.singletonList(dataset));
    QueryPlanVisitor<LogicalPlan, InputDataset> firstVisitor = first.asQueryPlanVisitor(event);
    QueryPlanVisitor<LogicalPlan, InputDataset> secondVisitor = second.asQueryPlanVisitor(event);
    List<String> trace = new ArrayList<>();
    Collection<InputDataset> results =
        DatasetDispatchTrace.capture(
            "input",
            100,
            trace::add,
            () -> PlanUtils.merge(Arrays.asList(firstVisitor, secondVisitor)).apply(node));
    assertThat(results).containsExactly(dataset);
    assertThat(first.applications).isEqualTo(1);
    assertThat(second.applications).isEqualTo(emptyFirst ? 1 : 0);
    assertThat(String.join("\n", trace)).contains("handler=" + RecordingBuilder.class.getName());
    if (!emptyFirst) {
      assertThat(String.join("\n", trace)).contains("visited=already-visited visitedBy=2");
    }
  }

  @SneakyThrows
  private static boolean throwUnchecked(Throwable error) {
    throw error;
  }

  static PartialFunction<Object, List<String>> handler(
      String name,
      List<String> calls,
      Predicate<Object> predicate,
      Function<Object, List<String>> application) {
    return new AbstractPartialFunction<Object, List<String>>() {
      @Override
      public boolean isDefinedAt(Object node) {
        calls.add(name + ".check");
        return predicate.test(node);
      }

      @Override
      public List<String> apply(Object node) {
        calls.add(name + ".apply");
        return application.apply(node);
      }
    };
  }

  private static final class RecordingBuilder
      extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LogicalPlan, InputDataset> {
    private final List<InputDataset> results;
    private int applications;

    private RecordingBuilder(OpenLineageContext context, List<InputDataset> results) {
      super(context, false);
      this.results = results;
    }

    @Override
    public boolean isDefinedAt(SparkListenerEvent event) {
      return true;
    }

    @Override
    protected boolean isDefinedAtLogicalPlan(LogicalPlan node) {
      return true;
    }

    @Override
    public List<InputDataset> apply(LogicalPlan node) {
      applications++;
      return results;
    }
  }
}
