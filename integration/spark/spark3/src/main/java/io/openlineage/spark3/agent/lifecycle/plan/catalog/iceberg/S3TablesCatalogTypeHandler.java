/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.S3TablesUtils;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Handles S3 Tables catalogs configured as native {@code S3TablesCatalog}, REST catalogs pointing
 * at the S3 Tables endpoint/signing name, REST catalogs using Glue federation IDs, and {@code
 * GlueCatalog} configured with an S3 Tables federation ID.
 */
class S3TablesCatalogTypeHandler extends BaseCatalogTypeHandler {

  private static final String S3TABLES_CATALOG_TYPE = "s3tables";

  @Override
  String getType() {
    return S3TABLES_CATALOG_TYPE;
  }

  @Override
  boolean matchesCatalogType(Map<String, String> catalogConf) {
    return S3TablesUtils.matchesS3TablesCatalogConfig(catalogConf);
  }

  @Override
  String getFacetType(Map<String, String> catalogConf) {
    return getType();
  }

  @Override
  Optional<DatasetIdentifier> getPrimaryIdentifier(
      SparkSession session,
      Map<String, String> catalogConf,
      Identifier identifier,
      String catalogName) {
    // S3 Tables data lives in AWS-managed physical buckets such as s3://...--table-s3.
    // That path is an implementation detail; lineage should use the user-facing S3 Tables
    // ARN plus the logical Spark catalog/namespace/table name.
    String[] namespace = identifier.namespace();
    StringBuilder nameBuilder = new StringBuilder(catalogName);
    for (String ns : namespace) {
      nameBuilder.append('.').append(ns);
    }
    nameBuilder.append('.').append(identifier.name());

    SparkContext ctx = session.sparkContext();
    String ns =
        S3TablesUtils.buildS3TablesArnFromCatalogConf(
            ctx.getConf(), ctx.hadoopConfiguration(), catalogConf);
    return Optional.of(new DatasetIdentifier(nameBuilder.toString(), ns));
  }

  @Override
  Optional<DatasetIdentifier> getIdentifier(
      SparkSession session, Map<String, String> catalogConf, String table) {
    // Kept for back-compat with the existing symlink dispatch in IcebergHandler;
    // not normally reached because getPrimaryIdentifier short-circuits the path.
    SparkContext ctx = session.sparkContext();
    String namespace =
        S3TablesUtils.buildS3TablesArnFromCatalogConf(
            ctx.getConf(), ctx.hadoopConfiguration(), catalogConf);
    return Optional.of(new DatasetIdentifier(table, namespace));
  }
}
