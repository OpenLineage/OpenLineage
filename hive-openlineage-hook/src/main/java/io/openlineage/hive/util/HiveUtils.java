/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveUtils {

  public static Table getTable(Configuration conf, String dbName, String tableName) {
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    try {
      return Hive.get(hiveConf).getTable(dbName, tableName);
    } catch (HiveException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Recursively processes an ASTNode and removes all TOK_IFNOTEXISTS nodes. This is needed because
   * for a query like "CREATE TABLE IF EXISTS xxx", when the hook gets executed, the table would
   * have already been created. And then, the SemanticAnalyzer would literally interpret the "IF NOT
   * EXISTS" statement and not generate any query plan.
   *
   * @param node The root node to process
   */
  public static void removeIfNotExistsStatements(ASTNode node) {
    if (node == null) {
      return;
    }

    // Get children list
    List<Node> children = node.getChildren();
    if (children == null || children.isEmpty()) {
      return;
    }

    // Traverse the children in reverse to avoid shifting the indexes
    // as we might remove a child during traversal
    for (int i = children.size() - 1; i >= 0; i--) {
      Node child = children.get(i);
      if (child instanceof ASTNode) {
        ASTNode childNode = (ASTNode) child;
        // Check if this child is TOK_IFNOTEXISTS
        if (childNode.getType() == HiveParser.TOK_IFNOTEXISTS) {
          node.deleteChild(i);
        } else {
          // Recursively process this child's children
          removeIfNotExistsStatements(childNode);
        }
      }
    }
  }

  @SneakyThrows
  public static DatasetIdentifier getDatasetIdentifierFromTable(Table table) {
    if (table.getParameters() != null && table.getParameters().get("bq.table") != null) {
      return new DatasetIdentifier(table.getParameters().get("bq.table"), "bigquery");
    }
    if (table.getSd() != null && table.getSd().getLocation() != null) {
      URI uri = new URI(table.getSd().getLocation());
      DatasetIdentifier di = FilesystemDatasetUtils.fromLocation(uri);
      return di.withSymlink(
          table.getFullyQualifiedName(), table.getCatName(), DatasetIdentifier.SymlinkType.TABLE);
    }
    return new DatasetIdentifier(table.getFullyQualifiedName(), table.getCatName());
  }

  public static SemanticAnalyzer analyzeQuery(
      Configuration conf, QueryState queryState, String queryString) {
    Context context;
    try {
      context = new Context(conf);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    context.setCmd(queryString);
    ExplainConfiguration explainConfig = new ExplainConfiguration();
    explainConfig.setAnalyze(ExplainConfiguration.AnalyzeState.ANALYZING);
    context.setExplainPlan(true);
    context.setExplainConfig(explainConfig);
    explainConfig.setOpIdToRuntimeNumRows(new HashMap<>());
    ASTNode tree;
    try {
      tree = ParseUtils.parse(queryString, context);
      removeIfNotExistsStatements(tree);
      SemanticAnalyzer semanticAnalyzer =
          (SemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, tree);
      semanticAnalyzer.analyze(tree, context);
      return semanticAnalyzer;
    } catch (ParseException | SemanticException e) {
      throw new IllegalStateException(e);
    }
  }
}
