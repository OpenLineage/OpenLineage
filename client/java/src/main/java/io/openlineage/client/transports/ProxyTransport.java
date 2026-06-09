/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.google.common.collect.MapMaker;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import scala.Function2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.openlineage.spark.agent.ArgumentParser.SPARK_CONF_PARENT_RUN_ID;

@Slf4j
@ToString
public final class ProxyTransport extends Transport implements ProxyTransportControllable {

  private final Transport underlying;
  private final Map<String, List<OpenLineage.DatasetEvent>> datasetEvents;
  private final Map<String, List<OpenLineage.RunEvent>> runEvents;
  private final Map<String, List<OpenLineage.JobEvent>> jobEvents;

  public ProxyTransport(@NonNull final Transport underlying) {
    this.underlying = underlying;
    this.runEvents = new MapMaker().weakKeys().weakValues().makeMap();
    this.jobEvents = new MapMaker().weakKeys().weakValues().makeMap();
    this.datasetEvents = new MapMaker().weakKeys().weakValues().makeMap();
    ProxyTransportRemote.setControllable(this);
  }


  @Override
  public void emit(@NonNull OpenLineage.RunEvent event) {
    putOrAppend(runEvents, calculateRunID(), event, this::mergeRuns);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent event) {
    putOrAppend(datasetEvents, calculateRunID(), event, this::mergeDatasets);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent event) {
    putOrAppend(jobEvents, calculateRunID(), event, this::mergeJobs);
  }

  @Override
  public void close() throws Exception {
    underlying.close();
  }

  /**
   * @return an new {@link Builder} object for building {@link ProxyTransport}s.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void emitAll() {
    emitAll(calculateRunID());
  }

  @Override
  public void emitAll(String runId) {
    var jobRecord = jobEvents.remove(runId);
    if (jobRecord != null) {
      if (underlying != null) {
        jobRecord.forEach(underlying::emit);
      } else {
        jobRecord.forEach(this::logEmittingEvent);
      }
    }

    var runRecord = runEvents.remove(runId);
    if (runRecord != null) {
      if (underlying != null) {
        runRecord.forEach(underlying::emit);
      } else {
        runRecord.forEach(this::logEmittingEvent);
      }
    }

    var datasetRecord = datasetEvents.remove(runId);
    if (datasetRecord != null) {
      if (underlying != null) {
        datasetRecord.forEach(underlying::emit);
      } else {
        datasetRecord.forEach(this::logEmittingEvent);
      }
    }

  }

  private OpenLineage.DatasetEvent mergeDatasets(OpenLineage.DatasetEvent left, OpenLineage.DatasetEvent right) {
    // TODO: implement method
    return left;
  }

  private OpenLineage.JobEvent mergeJobs(OpenLineage.JobEvent left, OpenLineage.JobEvent right) {
    // TODO: implement method
    return left;
  }

  private OpenLineage.RunEvent mergeRuns(OpenLineage.RunEvent left, OpenLineage.RunEvent right) {
    // TODO: implement method
    return left;
  }

  private <T extends OpenLineage.BaseEvent> void logEmittingEvent(T event) {
      log.debug("emitting event: {}", event.toString());
  }

  private <T extends OpenLineage.BaseEvent> List<T> putOrAppend(Map<String, List<T>> dict, String key, T event, Function2<T, T, T> merge) {
    var entry = dict.computeIfAbsent(key, k -> new LinkedList<>());
    entry.add(event);
    return entry;
  }

  private String calculateRunID() {
    assert SparkSessionUtils.activeSession().isPresent();
    return SparkSessionUtils
            .activeSession()
            .map(x -> x.conf().get(SPARK_CONF_PARENT_RUN_ID))
            .get();
  }

  /**
   * Builder for {@link ProxyTransport} instances.
   *
   */
  @Deprecated
  public static final class Builder {

    @Delegate private Transport transport;

    private Builder() {

    }

    public Builder underlying(@NonNull Transport transport) {
      return underlying(transport);
    }

    /**
     * @return an {@link ProxyTransport} object with the properties of this {@link
     *     Builder}.
     */
    public ProxyTransport build() {

      return new ProxyTransport(transport);
    }
  }
}
