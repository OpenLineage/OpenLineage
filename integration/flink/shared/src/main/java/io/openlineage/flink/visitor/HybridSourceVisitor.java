/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.hybrid.HybridSource.SourceFactory;

@Slf4j
public class HybridSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public static final String SOURCE_LIST_ENTRY_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource$SourceListEntry";
  public static final String PASS_THROUGH_SOURCE_FACTORY_CLASS =
      "org.apache.flink.connector.base.source.hybrid.HybridSource$PassthroughSourceFactory";
  private VisitorFactoryImpl visitorFactory = new VisitorFactoryImpl();

  public HybridSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return object instanceof HybridSource;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object object) {
    log.debug("Apply source {} in HybridSourceVisitor", object);
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    try {
      if (object instanceof HybridSource) {
        HybridSource hybridSource = (HybridSource) object;
        Class sourceListEntryClass = Class.forName(SOURCE_LIST_ENTRY_CLASS);
        Optional<List<Object>> sourceListOpt =
            WrapperUtils.<List<Object>>getFieldValue(HybridSource.class, hybridSource, "sources");
        if (sourceListOpt.isPresent()) {
          List<Object> sourceList = sourceListOpt.get();
          for (Object sourceEntry : sourceList) {
            Optional<SourceFactory> sourceFactoryOpt =
                WrapperUtils.<SourceFactory>getFieldValue(
                    sourceListEntryClass, sourceEntry, "factory");
            if (sourceListOpt.isPresent()) {
              Class passThroughSourceFactory = Class.forName(PASS_THROUGH_SOURCE_FACTORY_CLASS);
              Optional<Source> sourceOpt =
                  WrapperUtils.<Source>getFieldValue(
                      passThroughSourceFactory, sourceFactoryOpt.get(), "source");
              if (sourceOpt.isPresent()) {
                Source source = sourceOpt.get();
                visitorFactory.getInputVisitors(context).stream()
                    .filter(visitor -> visitor.isDefinedAt(source))
                    .forEach(visitor -> inputDatasets.addAll(visitor.apply(source)));
              }
            }
          }
        }
      }
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to find the sources added into Hybrid Source", e);
    }

    return inputDatasets;
  }
}
