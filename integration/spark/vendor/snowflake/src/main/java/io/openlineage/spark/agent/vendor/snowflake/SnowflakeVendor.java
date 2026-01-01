/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake;

import static io.openlineage.spark.agent.vendor.snowflake.Constants.SNOWFLAKE_PROVIDER_CLASS_NAME;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeRelationVisitor;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeVisitorFactory;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.SnowflakeEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;

public class SnowflakeVendor implements Vendor {

  public static boolean hasSnowflakeClasses() {
    /*
     Checking the Snowflake class with both
     SnowflakeRelationVisitor.class.getClassLoader.loadClass and
     Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
     present on the classpath, and the second one is a catchall which captures if the class has
     been installed. This is relevant for Azure Databricks where jars can be installed and
     accessible to the user, even if they are not present on the classpath.
    */
    try {
      SnowflakeRelationVisitor.class.getClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    return hasSnowflakeClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new SnowflakeVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new SnowflakeEventHandlerFactory());
  }
}
