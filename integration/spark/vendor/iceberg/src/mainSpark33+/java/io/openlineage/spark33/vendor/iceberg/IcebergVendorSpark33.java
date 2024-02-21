/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.vendor.iceberg;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import io.openlineage.spark33.vendor.iceberg.agent.lifecycle.plan.ReplaceIcebergDataDatasetBuilder;
import io.openlineage.spark34.vendor.iceberg.agent.lifecycle.plan.column.MergeIntoIceberg13ColumnLineageVisitor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;

@Slf4j
public class IcebergVendorSpark33 implements Vendor {

  private static final AtomicBoolean ICEBERG_PROVIDER_CLASS_PRESENT = new AtomicBoolean(false);
  private static final AtomicBoolean ICEBERG_PROVIDER_CHECKED = new AtomicBoolean(false);
  private static final String ICEBERG_SOURCE_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData";

  @Override
  public boolean isVendorAvailable() {
    log.debug("Checking if Iceberg Spark33+ classes are available");
    if (!ICEBERG_PROVIDER_CHECKED.get()) {
      log.debug("Iceberg Spark33+ classes have not been checked yet");
      synchronized (IcebergVendorSpark33.class) {
        ICEBERG_PROVIDER_CLASS_PRESENT.set(
            ReflectionUtils.hasClassWithCheck(
                ICEBERG_PROVIDER_CHECKED,
                IcebergVendorSpark33.class,
                ICEBERG_SOURCE_PROVIDER_CLASS_NAME));
      }
    }
    return ICEBERG_PROVIDER_CLASS_PRESENT.get();
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(
        new OpenLineageEventHandlerFactory() {

          @Override
          public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
              createOutputDatasetBuilder(OpenLineageContext context) {
            return Collections.singletonList(
                (PartialFunction) new ReplaceIcebergDataDatasetBuilder(context));
          }

          @Override
          public Collection<ColumnLevelLineageVisitor> createColumnLevelLineageVisitors(
              OpenLineageContext context) {
            return Collections.singletonList(new MergeIntoIceberg13ColumnLineageVisitor(context));
          }
        });
  }
}
