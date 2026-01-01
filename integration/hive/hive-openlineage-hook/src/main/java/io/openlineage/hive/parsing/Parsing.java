/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;

public class Parsing {

  public static QueryExpr buildQueryTree(QB qb, String outputTable) {
    QueryExpr query = new QueryExpr(qb.getId());

    // Add the defined tables, if any, as subqueries
    for (String tabAlias : qb.getTabAliases()) {
      QueryExpr tableQuery = new QueryExpr(tabAlias);
      Table table = qb.getMetaData().getSrcForAlias(tabAlias);
      List<FieldSchema> allCols = new ArrayList<>();
      allCols.addAll(table.getCols());
      allCols.addAll(table.getPartCols());
      for (int i = 0; i < allCols.size(); i++) {
        FieldSchema fieldSchema = allCols.get(i);
        ColumnExpr column = new ColumnExpr(fieldSchema.getName().toLowerCase(Locale.ROOT));
        column.setIndex(i);
        column.setTable(table);
        tableQuery.addSelectExpression(fieldSchema.getName(), column);
      }
      query.getSubQueries().computeIfAbsent(tabAlias, k -> new ArrayList<>()).add(tableQuery);
    }

    // Add the other subqueries, if any
    for (String subAlias : qb.getSubqAliases()) {
      QBExpr qbExpr = qb.getSubqForAlias(subAlias);
      query.getSubQueries().computeIfAbsent(subAlias, k -> new ArrayList<>());
      if (qbExpr.getOpcode().equals(QBExpr.Opcode.NULLOP)) {
        QB subQB = qbExpr.getQB();
        query.getSubQueries().get(subAlias).add(buildQueryTree(subQB, outputTable));
      } else {
        // This could be, for example, a UNION query
        QB subQB1 = qbExpr.getQBExpr1().getQB();
        query.getSubQueries().get(subAlias).add(buildQueryTree(subQB1, outputTable));
        QB subQB2 = qbExpr.getQBExpr2().getQB();
        query.getSubQueries().get(subAlias).add(buildQueryTree(subQB2, outputTable));
      }
    }

    // Parse the expressions in the query clauses (SELECT, JOIN, GROUP BY, etc.)
    Parsing.parseQueryExpressions(qb, outputTable, query);
    return query;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static String getClauseName(QB qb, String outputTable) {
    QBParseInfo parseInfo = qb.getParseInfo();
    if (parseInfo.getClauseNamesForDest().isEmpty()) {
      throw new IllegalArgumentException(
          "The query doesn't have any clause name. There must have been a parsing issue.");
    }
    if (parseInfo.getClauseNamesForDest().size() == 1) {
      return (String) parseInfo.getClauseNamesForDest().toArray()[0];
    }
    // There are multiple clause names. This happens with a query that
    // has multiple INSERT statements. So we try to find the statement
    // that corresponds to the given output table.
    List<Table> destTables = new ArrayList<>();
    for (String clauseName : parseInfo.getClauseNamesForDest()) {
      Table destTable = qb.getMetaData().getNameToDestTable().get(clauseName);
      destTables.add(destTable);
      if (outputTable.equals(destTable.getFullyQualifiedName())) {
        return clauseName;
      }
    }
    throw new IllegalArgumentException(
        "Could not match output table `"
            + outputTable
            + "` with: "
            + destTables.stream()
                .map(table -> "`" + table.getFullyQualifiedName() + "`")
                .collect(Collectors.joining(", ")));
  }

  public static void parseQueryExpressions(QB qb, String outputTable, QueryExpr query) {
    String clauseName = getClauseName(qb, outputTable);

    // Parse the SELECT expressions
    QBParseInfo parseInfo = qb.getParseInfo();
    ASTNode selectNode = parseInfo.getSelForClause(clauseName);
    Parsing.parseSelectExpressions(query, selectNode);

    // Parse the JOIN expressions
    Parsing.parseJoinExpressions(query, qb);

    // Parse the WHERE expressions (i.e. the query filters)
    ASTNode whereNode = parseInfo.getWhrForClause(clauseName);
    Parsing.parseWhereExpressions(query, whereNode);

    // Parse the GROUP BY expressions
    ASTNode groupByNode = parseInfo.getGroupByForClause(clauseName);
    Parsing.parseGroupByExpressions(query, groupByNode);

    // Parse the ORDER BY expressions (i.e. sorting columns)
    ASTNode orderByNode = parseInfo.getOrderByForClause(clauseName);
    Parsing.parseOrderByExpressions(query, orderByNode);
  }

  public static void parseGroupByExpressions(QueryExpr query, ASTNode groupByNode) {
    if (groupByNode == null) {
      return;
    }
    List<BaseExpr> expressions = new ArrayList<>();
    for (Node child : groupByNode.getChildren()) {
      if (child instanceof ASTNode) {
        ASTNode groupByItem = (ASTNode) child;
        BaseExpr columnExpr = getParsedExpr(query, groupByItem);
        if (columnExpr != null) {
          expressions.add(columnExpr);
        }
      }
    }
    query.getGroupByExpressions().addAll(expressions);
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public static void parseSelectExpressions(QueryExpr query, ASTNode selectNode) {
    for (Node child : selectNode.getChildren()) {
      ASTNode astChild = (ASTNode) child;
      if (astChild.getChildCount() > 1
          && astChild.getChild(astChild.getChildCount() - 1).getType() == HiveParser.Identifier) {
        // This is an aliased column
        String alias = astChild.getChild(astChild.getChildCount() - 1).getText();
        BaseExpr expr = getParsedExpr(query, (ASTNode) astChild.getChild(0));
        query.addSelectExpression(alias.toLowerCase(Locale.ROOT), expr);
      } else if (astChild.getChildCount() == 1) {
        // This is a non-aliased node
        BaseExpr expr = getParsedExpr(query, (ASTNode) astChild.getChild(0));
        if (expr instanceof StarExpr) {
          // This is a "SELECT *"
          StarExpr starExpr = (StarExpr) expr;
          query.addSelectExpressions(starExpr.getAliases(), starExpr.getChildren());
        } else if (expr instanceof ColumnExpr) {
          ColumnExpr column = (ColumnExpr) expr;
          query.addSelectExpression(column.getName().toLowerCase(Locale.ROOT), column);
        } else if (expr instanceof UDTFExpr
            && ((UDTFExpr) expr).getFunction() instanceof GenericUDTFInline
            && expr.getChildren().get(0) instanceof FunctionExpr
            && ((FunctionExpr) expr.getChildren().get(0)).getFunction()
                instanceof GenericUDFStruct) {
          // This is a VALUES(...) statement
          FunctionExpr array = ((FunctionExpr) expr.getChildren().get(0));
          for (int i = 0; i < array.getChildren().size(); i++) {
            query.addSelectExpression("_" + i, array.getChildren().get(i));
          }
        } else {
          // Nameless expression
          query.addSelectExpression("_" + query.getSelectExpressions().size(), expr);
        }
      } else {
        throw new IllegalStateException("Unable to get expression from node: " + astChild);
      }
    }
  }

  public static void parseJoinExpressions(QueryExpr query, QB qb) {
    if (qb.getQbJoinTree() == null
        || qb.getQbJoinTree().getExpressions() == null
        || qb.getQbJoinTree().getExpressions().isEmpty()) {
      return;
    }
    List<BaseExpr> expressions = new ArrayList<>();
    for (List<ASTNode> joinNodes : qb.getQbJoinTree().getExpressions()) {
      for (ASTNode joinNode : joinNodes) {
        BaseExpr expr = getParsedExpr(query, joinNode);
        expressions.add(expr);
      }
    }
    query.getJoinExpressions().addAll(expressions);
  }

  public static void parseWhereExpressions(QueryExpr query, ASTNode whereNode) {
    if (whereNode == null) {
      return;
    }
    List<BaseExpr> expressions = new ArrayList<>();
    for (Node childNode : whereNode.getChildren()) {
      BaseExpr expr = getParsedExpr(query, (ASTNode) childNode);
      expressions.add(expr);
    }
    query.getWhereExpressions().addAll(expressions);
  }

  public static void parseOrderByExpressions(QueryExpr query, ASTNode orderByNode) {
    if (orderByNode == null) {
      return;
    }
    List<BaseExpr> expressions = new ArrayList<>();
    for (Node childNode : orderByNode.getChildren()) {
      ASTNode orderByColumn = extractOrderByColumn((ASTNode) childNode);
      BaseExpr expr = getParsedExpr(query, orderByColumn);
      expressions.add(expr);
    }
    query.getOrderByExpressions().addAll(expressions);
  }

  public static ASTNode extractOrderByColumn(ASTNode node) {
    if (node.getType() == HiveParser.TOK_TABSORTCOLNAMEASC
        || node.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC
        || node.getType() == HiveParser.TOK_NULLS_FIRST
        || node.getType() == HiveParser.TOK_NULLS_LAST) {
      return extractOrderByColumn((ASTNode) node.getChild(0));
    }
    return node;
  }

  public static BaseExpr getParsedExpr(QueryExpr query, ASTNode node) {
    switch (node.getType()) {
      case HiveParser.TOK_TABLE_OR_COL:
        return handleUnqualifiedColumn(query, node);
      case HiveParser.TOK_FUNCTION:
        return handleFunction(query, node);
      case HiveParser.DOT:
        return handleTableDotColumn(query, node);
      case HiveParser.TOK_ALLCOLREF:
        return handleStar(query);
      case HiveParser.TOK_WINDOWSPEC:
        return handleWindow(query, node);
      default:
        return handleGenericExpression(query, node);
    }
  }

  public static BaseExpr handleGenericExpression(QueryExpr query, ASTNode node) {
    if (node.getChildren() == null) {
      return new GenericExpr(new ArrayList<>());
    }
    List<BaseExpr> childrenExpressions = new ArrayList<>();
    for (Node child : node.getChildren()) {
      childrenExpressions.add(getParsedExpr(query, (ASTNode) child));
    }
    return new GenericExpr(childrenExpressions);
  }

  public static BaseExpr handleStar(QueryExpr query) {
    List<String> aliases = new ArrayList<>();
    List<BaseExpr> children = new ArrayList<>();
    for (String subqueryAlias : query.getSubQueries().keySet()) {
      QueryExpr subQuery = query.getSubQueries().get(subqueryAlias).get(0);
      aliases.addAll(subQuery.getSelectAliases());
      children.addAll(subQuery.getSelectExpressions());
    }
    return new StarExpr(aliases, children);
  }

  public static BaseExpr handleTableDotColumn(QueryExpr query, ASTNode node) {
    String columnName = node.getChild(1).getText();
    String tabAlias = node.getChild(0).getChild(0).getText();
    List<QueryExpr> subQueries = query.getSubQueries().get(tabAlias);
    int columnIndex =
        subQueries.get(0).getSelectAliases().indexOf(columnName.toLowerCase(Locale.ROOT));
    ColumnExpr column = new ColumnExpr(columnName.toLowerCase(Locale.ROOT));
    column.setIndex(columnIndex);
    column.setQueries(subQueries);
    return column;
  }

  public static BaseExpr handleUnqualifiedColumn(QueryExpr query, ASTNode node) {
    String columnName = node.getChild(0).getText();
    // Look in the subqueries
    for (String subAlias : query.getSubQueries().keySet()) {
      List<QueryExpr> subQueries = query.getSubQueries().get(subAlias);
      int columnIndex =
          subQueries.get(0).getSelectAliases().indexOf(columnName.toLowerCase(Locale.ROOT));
      if (columnIndex != -1) {
        ColumnExpr column = new ColumnExpr(columnName.toLowerCase(Locale.ROOT));
        column.setIndex(columnIndex);
        column.setQueries(subQueries);
        return column;
      }
    }
    // Look in the select expressions
    int columnIndex = query.getSelectAliases().indexOf(columnName.toLowerCase(Locale.ROOT));
    if (columnIndex != -1) {
      ColumnExpr column = new ColumnExpr(columnName.toLowerCase(Locale.ROOT));
      column.setIndex(columnIndex);
      column.setExpression(query.getSelectExpressions().get(columnIndex));
      return column;
    }
    throw new IllegalStateException("Column not found: " + columnName);
  }

  public static BaseExpr handleWindow(QueryExpr query, ASTNode node) {
    List<BaseExpr> childrenExpressions = new ArrayList<>();
    for (Node child : node.getChildren()) {
      childrenExpressions.add(getParsedExpr(query, (ASTNode) child));
    }
    return new WindowExpr(childrenExpressions);
  }

  public static BaseExpr handleFunction(QueryExpr query, ASTNode node) {
    String functionName = node.getChild(0).getText().toLowerCase(Locale.ROOT);
    FunctionInfo functionInfo;
    try {
      functionInfo = FunctionRegistry.getFunctionInfo(functionName);
    } catch (SemanticException e) {
      throw new IllegalStateException(e);
    }
    ArrayList<BaseExpr> childrenExpressions = new ArrayList<>();
    for (int i = 1; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      BaseExpr childExpr = getParsedExpr(query, child);
      if (childExpr == null) {
        childExpr = getParsedExpr(query, child);
      }
      childrenExpressions.add(childExpr);
    }
    if (functionInfo == null) {
      return new FunctionExpr(functionName, null, childrenExpressions);
    }
    if (functionInfo.isGenericUDAF()) {
      return new AggregateExpr(functionInfo.getGenericUDAFResolver(), childrenExpressions);
    } else if (functionInfo.isGenericUDF()) {
      if (functionInfo.getGenericUDF().getClass().equals(GenericUDFWhen.class)) {
        return new IfExpr(childrenExpressions);
      }
      if (functionInfo.getGenericUDF().getClass().equals(GenericUDFIf.class)) {
        return new CaseWhenExpr(childrenExpressions);
      }
      return new FunctionExpr(functionName, functionInfo.getGenericUDF(), childrenExpressions);
    } else if (functionInfo.isGenericUDTF()) {
      return new UDTFExpr(functionInfo.getGenericUDTF(), childrenExpressions.get(0).getChildren());
    }
    return new FunctionExpr(functionName, null, childrenExpressions);
  }
}
