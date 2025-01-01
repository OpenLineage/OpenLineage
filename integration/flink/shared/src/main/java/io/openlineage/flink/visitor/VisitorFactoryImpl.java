/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.ClassUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VisitorFactoryImpl implements VisitorFactory {
  @Override
  public List<Visitor<OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context) {
    ArrayList<Visitor<InputDataset>> visitors =
        new ArrayList(
            Arrays.asList(
                new KafkaSourceVisitor(context),
                new FlinkKafkaConsumerVisitor(context),
                new LineageProviderVisitor<>(context, DatasetFactory.input(context))));

    if (ClassUtils.hasIcebergClasses()) {
      visitors.add(new IcebergSourceVisitor(context));
    }

    if (ClassUtils.hasCassandraClasses()) {
      visitors.add(new CassandraSourceVisitor(context));
    }

    if (ClassUtils.hasJdbcClasses()) {
      visitors.add(new JdbcSourceVisitor(context));
    }

    visitors.add(new HybridSourceVisitor(context));

    return Collections.unmodifiableList(visitors);
  }

  @Override
  public List<Visitor<OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context) {
    ArrayList<Visitor<OutputDataset>> visitors =
        new ArrayList(
            Arrays.asList(
                new KafkaSinkVisitor(context),
                new FlinkKafkaProducerVisitor(context),
                new LineageProviderVisitor<>(context, DatasetFactory.output(context))));

    if (ClassUtils.hasIcebergClasses()) {
      visitors.add(new IcebergSinkVisitor(context));
    }

    if (ClassUtils.hasCassandraClasses()) {
      visitors.add(new CassandraSinkVisitor(context));
    }

    if (ClassUtils.hasJdbcClasses()) {
      visitors.add(new JdbcSinkVisitor(context));
    }

    return Collections.unmodifiableList(visitors);
  }
}
