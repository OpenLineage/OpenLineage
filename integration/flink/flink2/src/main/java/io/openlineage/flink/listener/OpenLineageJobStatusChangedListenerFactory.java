/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.facet.TableLineageFacetVisitor;
import io.openlineage.flink.visitor.facet.TypeDatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import io.openlineage.flink.visitor.identifier.JdbcTableLineageDatasetIdentifierVisitor;
import io.openlineage.flink.visitor.identifier.KafkaTableLineageDatasetIdentifierVisitor;
import io.openlineage.flink.visitor.identifier.KafkaTopicListDatasetIdentifierVisitor;
import io.openlineage.flink.visitor.identifier.KafkaTopicPatternDatasetIdentifierVisitor;
import java.util.Arrays;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;

@Slf4j
public class OpenLineageJobStatusChangedListenerFactory implements JobStatusChangedListenerFactory {

  @Override
  public JobStatusChangedListener createListener(Context context) {
    log.info(
        "Creating OpenLineageJobStatusChangedListener with Flink configuration: {}",
        context.getConfiguration());
    return new OpenLineageJobStatusChangedListener(context, loadVisitorFactory());
  }

  Flink2VisitorFactory loadVisitorFactory() {
    return new Flink2VisitorFactory() {
      @Override
      public Collection<DatasetFacetVisitor> loadDatasetFacetVisitors(OpenLineageContext context) {
        return Arrays.asList(
            new TypeDatasetFacetVisitor(context), new TableLineageFacetVisitor(context));
      }

      @Override
      public Collection<DatasetIdentifierVisitor> loadDatasetIdentifierVisitors(
          OpenLineageContext context) {
        return Arrays.asList(
            new KafkaTopicPatternDatasetIdentifierVisitor(context),
            new KafkaTopicListDatasetIdentifierVisitor(),
            new JdbcTableLineageDatasetIdentifierVisitor(),
            new KafkaTableLineageDatasetIdentifierVisitor());
      }
    };
  }
}
