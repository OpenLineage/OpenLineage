/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.IcebergUtils;
import java.util.List;

public class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<Visitor<OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context) {
    Builder<Visitor<InputDataset>> builder = ImmutableList.<Visitor<InputDataset>>builder();

    builder.add(new KafkaSourceVisitor(context));
    builder.add(new FlinkKafkaConsumerVisitor(context));
    builder.add(
        new LineageProviderVisitor<>(context, DatasetFactory.input(context.getOpenLineage())));

    if (IcebergUtils.hasClasses()) {
      builder.add(new IcebergSourceVisitor(context));
    }

    return builder.build();
  }

  @Override
  public List<Visitor<OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context) {
    Builder<Visitor<OutputDataset>> builder = ImmutableList.<Visitor<OutputDataset>>builder();

    builder.add(new KafkaSinkVisitor(context));
    builder.add(new FlinkKafkaProducerVisitor(context));
    builder.add(
        new LineageProviderVisitor<>(context, DatasetFactory.output(context.getOpenLineage())));

    if (IcebergUtils.hasClasses()) {
      builder.add(new IcebergSinkVisitor(context));
    }

    return builder.build();
  }
}
