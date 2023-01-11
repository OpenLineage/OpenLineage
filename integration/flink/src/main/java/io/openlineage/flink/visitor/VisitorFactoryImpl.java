/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;

public class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<Visitor<OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context) {
    return Arrays.asList(
        new KafkaSourceVisitor(context),
        new FlinkKafkaConsumerVisitor(context),
        new IcebergSourceVisitor(context),
        new LineageProviderVisitor<>(context, DatasetFactory.input(context.getOpenLineage())));
  }

  @Override
  public List<Visitor<OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context) {
    return Arrays.asList(
        new KafkaSinkVisitor(context),
        new FlinkKafkaProducerVisitor(context),
        new LineageProviderVisitor<>(context, DatasetFactory.output(context.getOpenLineage())));
  }
}
