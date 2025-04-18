/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark2.agent.lifecycle.plan;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTable$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;

public class SparkUtils {
  // Can't use Scala's default parameters from Java.
  public static CatalogTable catalogTable(
      TableIdentifier identifier,
      CatalogTableType tableType,
      CatalogStorageFormat storageFormat,
      StructType schema) {
    return CatalogTable$.MODULE$.apply(
        identifier,
        tableType,
        storageFormat,
        schema,
        Option.apply("provider"),
        Seq$.MODULE$.<String>empty(),
        Option.empty(),
        "",
        System.currentTimeMillis(),
        -1L,
        "",
        Map$.MODULE$.empty(),
        Option.empty(),
        Option.empty(),
        Option.empty(),
        Seq$.MODULE$.<String>empty(),
        false,
        true,
        Map$.MODULE$.empty());
  }
}
