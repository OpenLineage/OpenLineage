/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetEvent;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransformTransport extends Transport {

  @Getter private final Transport transport;
  @Getter private final EventTransformer transformer;

  public TransformTransport(@NonNull TransformConfig config) {
    super();
    this.transport = TransportResolver.resolveTransportByConfig(config.getTransport());
    this.transformer = initializeTransformClass(config);
  }

  public TransformTransport(@NonNull TransformConfig config, Transport transport) {
    super();
    this.transport = transport;
    this.transformer = initializeTransformClass(config);
  }

  private EventTransformer initializeTransformClass(TransformConfig config) {
    try {
      Class<?> transformerClass = Class.forName(config.getTransformerClass());
      EventTransformer instance =
          (EventTransformer) transformerClass.getConstructor().newInstance();
      instance.initialize(config.getTransformerProperties());
      return instance;
    } catch (ClassNotFoundException e) {
      throw new TransformTransportException("Cannot find transformer class", e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new TransformTransportException("Cannot instantiate transformer class", e);
    } catch (ClassCastException e) {
      throw new TransformTransportException("Transform class not an EventTransformer", e);
    } catch (Exception e) {
      throw new TransformTransportException("Error initializing transformer class", e);
    }
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    RunEvent updatedRunEvent;
    try {
      updatedRunEvent = transformer.transform(runEvent);
    } catch (Exception e) {
      throw new TransformTransportException("Error transforming RunEvent", e);
    }
    if (updatedRunEvent == null) {
      log.warn("Transformed RunEvent is null, not emitting");
      return;
    }
    transport.emit(updatedRunEvent);
  }

  @Override
  public void emit(@NonNull DatasetEvent datasetEvent) {
    DatasetEvent updatedDatasetEvent;
    try {
      updatedDatasetEvent = transformer.transform(datasetEvent);
    } catch (Exception e) {
      throw new TransformTransportException("Error transforming DatasetEvent", e);
    }
    if (updatedDatasetEvent == null) {
      log.warn("Transformed DatasetEvent is null, not emitting");
      return;
    }
    transport.emit(updatedDatasetEvent);
  }

  @Override
  public void emit(@NonNull JobEvent jobEvent) {
    JobEvent updatedJobEvent;
    try {
      updatedJobEvent = transformer.transform(jobEvent);
    } catch (Exception e) {
      throw new TransformTransportException("Error transforming JobEvent", e);
    }
    if (updatedJobEvent == null) {
      log.warn("Transformed JobEvent is null, not emitting");
      return;
    }
    transport.emit(updatedJobEvent);
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }
}
