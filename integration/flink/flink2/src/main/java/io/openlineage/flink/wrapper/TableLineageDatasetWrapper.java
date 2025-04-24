/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.wrapper;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.listener.CatalogContext;

/**
 * Some of the visitors rely on org.apache.flink.table.planner.lineage.TableLineageDataset class,
 * which is included in table-planner module. There is some classloading issue with this and the OL
 * job listener is not able to use this class (NoClassDefFoundError) nor even load it through class
 * loader. This class is a wrapper around TableLineageDataset to avoid classloading issues.
 *
 * <p>Reflection should be avoided. However, no better solution has been found. The root cause is
 * probably on the flink side, as lineage interfaces should not be put into a module that is not
 * available to the job listener.
 *
 * <p>Including table-planner module in the job listener package does not solve the issue:
 * TableLineageDatasetImpl object from the lineage interface is not an instance of
 * TableLineageDataset manually included on the classpath.
 */
@Slf4j
public class TableLineageDatasetWrapper {

  private final LineageDataset dataset;

  public TableLineageDatasetWrapper(LineageDataset dataset) {
    this.dataset = dataset;
  }

  public boolean isTableLineageDataset() {
    return dataset
        .getClass()
        .getName()
        .startsWith("org.apache.flink.table.planner.lineage.TableLineageDataset");
  }

  public Optional<CatalogBaseTable> getTable() {
    CatalogBaseTable table = null;
    if (isTableLineageDataset()) {
      try {
        table = (CatalogBaseTable) MethodUtils.invokeMethod(dataset, "table");
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        log.warn("Error invoking table method on dataset {}", dataset, e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.error("Error invoking table method on dataset {}", dataset, e);
      }
    }
    return Optional.ofNullable(table);
  }

  public Optional<CatalogContext> getCatalogContext() {
    CatalogContext context = null;
    if (isTableLineageDataset()) {
      try {
        context = (CatalogContext) MethodUtils.invokeMethod(dataset, "catalogContext");
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        log.warn("Error invoking catalogContext method on dataset {}", dataset, e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.error("Error invoking catalogContext method on dataset {}", dataset, e);
      }
    }
    return Optional.ofNullable(context);
  }
}
