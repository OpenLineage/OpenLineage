/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.visitor.DatasetFacetVisitor;
import io.openlineage.flink.visitor.DatasetIdentifierVisitor;
import io.openlineage.flink.visitor.KafkaTableLineageDatasetIdentifierVisitor;
import io.openlineage.flink.visitor.KafkaTopicPatternDatasetIdentifierVisitor;
import io.openlineage.flink.visitor.TableLineageFacetVisitor;
import io.openlineage.flink.visitor.TypeInformationFacetVisitor;
import io.openlineage.flink.visitor.VisitorFactory;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;

public class OpenLineageJobStatusChangedListenerFactory implements JobStatusChangedListenerFactory {

  @Override
  public JobStatusChangedListener createListener(Context context) {
    // TODO: Write test which verifies listener factory created via manifest file
    // This can be done once Docker image with flink version supporting lineage graph extraction
    // is released
    return new OpenLineageFlinkListener(context, loadVisitorFactory());
  }

  VisitorFactory loadVisitorFactory() {
    return new VisitorFactory() {
      @Override
      public Collection<DatasetFacetVisitor> loadDatasetFacetVisitors(OpenLineageContext context) {
        return Arrays.asList(
            new TypeInformationFacetVisitor(context), new TableLineageFacetVisitor(context));
      }

      @Override
      public Collection<DatasetIdentifierVisitor> loadDatasetIdentifierVisitors(
          OpenLineageContext context) {
        return Arrays.asList(
            new KafkaTopicPatternDatasetIdentifierVisitor(context),
            new KafkaTableLineageDatasetIdentifierVisitor());
      }
    };
  }
}
