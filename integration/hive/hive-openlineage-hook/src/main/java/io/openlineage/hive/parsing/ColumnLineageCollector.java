/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.parsing;

import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.SORT;
import static io.openlineage.hive.hooks.TransformationInfo.Subtypes.WINDOW;

import io.openlineage.hive.hooks.OutputCLL;
import io.openlineage.hive.hooks.TransformationInfo;
import io.openlineage.hive.udf.interfaces.UDFAdditionalLineage;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.udf.UDFCrc32;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.UDFSha1;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSha2;

public class ColumnLineageCollector {

  // Generally available masking expressions
  private static final List<Class<?>> MASKING_CLASSES =
      Arrays.asList(
          UDFCrc32.class,
          UDFMd5.class,
          GenericUDFMurmurHash.class,
          UDFSha1.class,
          GenericUDFSha2.class,
          GenericUDFMaskHash.class,
          GenericUDAFCount.class);

  public static OutputCLL collectCLL(QueryExpr query, Table outputTable) {
    OutputCLL outputCLL = new OutputCLL(outputTable);
    List<FieldSchema> fieldSchemas = outputTable.getCols();

    // Collect the direct column dependencies
    for (int i = 0; i < fieldSchemas.size(); i++) {
      String outputColumn = fieldSchemas.get(i).getName();
      BaseExpr selectExpression = query.getSelectExpressions().get(i);
      traverseExpression(outputCLL, outputColumn, selectExpression, TransformationInfo.identity());
    }

    // Collect the dataset (i.e. indirect) dependencies
    collectDatasetDependencies(
        outputCLL, query.getJoinExpressions(), TransformationInfo.indirect(JOIN));
    collectDatasetDependencies(
        outputCLL, query.getGroupByExpressions(), TransformationInfo.indirect(GROUP_BY));
    collectDatasetDependencies(
        outputCLL, query.getOrderByExpressions(), TransformationInfo.indirect(SORT));
    collectDatasetDependencies(
        outputCLL, query.getWhereExpressions(), TransformationInfo.indirect(FILTER));

    return outputCLL;
  }

  static void collectDatasetDependencies(
      OutputCLL outputCLL, List<BaseExpr> exprs, TransformationInfo transformationInfo) {
    for (BaseExpr expr : exprs) {
      traverseExpression(outputCLL, null, expr, transformationInfo);
    }
  }

  static void traverseExpression(
      OutputCLL outputCLL,
      String outputColumn,
      BaseExpr expr,
      TransformationInfo transformationInfo) {
    if (expr instanceof ColumnExpr) {
      handleColumnExpression(outputCLL, outputColumn, (ColumnExpr) expr, transformationInfo);
    } else if (expr instanceof AggregateExpr) {
      handleAggregateExpression(outputCLL, outputColumn, (AggregateExpr) expr, transformationInfo);
    } else if (expr instanceof WindowExpr) {
      handleWindowExpression(outputCLL, outputColumn, (WindowExpr) expr, transformationInfo);
    } else if (expr instanceof FunctionExpr) {
      handleFunctionExpression(outputCLL, outputColumn, (FunctionExpr) expr, transformationInfo);
    } else if (expr instanceof UDTFExpr) {
      handleUDTFExpression(outputCLL, outputColumn, (UDTFExpr) expr, transformationInfo);
    } else if (expr instanceof CaseWhenExpr) {
      handleCaseWhenExpression(outputCLL, outputColumn, (CaseWhenExpr) expr, transformationInfo);
    } else if (expr instanceof IfExpr) {
      handleIfExpression(outputCLL, outputColumn, (IfExpr) expr, transformationInfo);
    } else if (expr instanceof GenericExpr) {
      handleGenericExpression(outputCLL, outputColumn, (GenericExpr) expr, transformationInfo);
    } else {
      // TODO: NULLIF, NVL, NVL2, COALESCE
      throw new IllegalStateException("Unsupported expression: " + expr);
    }
  }

  static void handleQueryExpression(
      OutputCLL outputCLL,
      String outputColumn,
      QueryExpr query,
      TransformationInfo transformationInfo,
      int columnIndex) {
    traverseExpression(
        outputCLL, outputColumn, query.getSelectExpressions().get(columnIndex), transformationInfo);
    collectDatasetDependencies(
        outputCLL, query.getJoinExpressions(), TransformationInfo.indirect(JOIN));
    collectDatasetDependencies(
        outputCLL, query.getGroupByExpressions(), TransformationInfo.indirect(GROUP_BY));
    collectDatasetDependencies(
        outputCLL, query.getOrderByExpressions(), TransformationInfo.indirect(SORT));
    collectDatasetDependencies(
        outputCLL, query.getWhereExpressions(), TransformationInfo.indirect(FILTER));
  }

  static void handleColumnExpression(
      OutputCLL outputCLL,
      String outputColumn,
      ColumnExpr column,
      TransformationInfo transformationInfo) {
    if (column.getTable() != null) {
      String inputColumn = column.getTable().getFullyQualifiedName() + "." + column.getName();
      outputCLL.getInputTables().put(column.getTable().getFullyQualifiedName(), column.getTable());
      if (outputColumn == null) {
        outputCLL
            .getDatasetDependencies()
            .computeIfAbsent(inputColumn, k -> new HashSet<>())
            .add(transformationInfo);
      } else {
        outputCLL
            .getColumnDependencies()
            .get(outputColumn)
            .computeIfAbsent(inputColumn, k -> new HashSet<>())
            .add(transformationInfo);
      }
    } else if (column.getExpression() != null) {
      traverseExpression(outputCLL, outputColumn, column.getExpression(), transformationInfo);
    } else {
      for (QueryExpr query : column.getQueries()) {
        handleQueryExpression(
            outputCLL, outputColumn, query, transformationInfo, column.getIndex());
      }
    }
  }

  static void handleWindowExpression(
      OutputCLL outputCLL,
      String outputColumn,
      WindowExpr window,
      TransformationInfo transformationInfo) {
    for (BaseExpr child : window.getChildren()) {
      traverseExpression(
          outputCLL,
          outputColumn,
          child,
          transformationInfo.merge(TransformationInfo.indirect(WINDOW)));
    }
  }

  static void handleAggregateExpression(
      OutputCLL outputCLL,
      String outputColumn,
      AggregateExpr agg,
      TransformationInfo transformationInfo) {
    TransformationInfo updatedTransformationInfo =
        transformationInfo.merge(TransformationInfo.aggregation(isMasking(agg)));
    for (BaseExpr child : agg.getChildren()) {
      traverseExpression(outputCLL, outputColumn, child, updatedTransformationInfo);
    }
  }

  static void handleFunctionExpression(
      OutputCLL outputCLL,
      String outputColumn,
      FunctionExpr func,
      TransformationInfo transformationInfo) {
    TransformationInfo updatedTransformationInfo =
        transformationInfo.merge(TransformationInfo.transformation(isMasking(func)));
    for (BaseExpr child : func.getChildren()) {
      traverseExpression(outputCLL, outputColumn, child, updatedTransformationInfo);
    }
  }

  static void handleUDTFExpression(
      OutputCLL outputCLL,
      String outputColumn,
      UDTFExpr udtf,
      TransformationInfo transformationInfo) {
    TransformationInfo updatedTransformationInfo =
        transformationInfo.merge(TransformationInfo.transformation(isMasking(udtf)));
    for (BaseExpr child : udtf.getChildren()) {
      traverseExpression(outputCLL, outputColumn, child, updatedTransformationInfo);
    }
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  static void handleCaseWhenExpression(
      OutputCLL outputCLL,
      String outputColumn,
      CaseWhenExpr expr,
      TransformationInfo transformationInfo) {
    if (expr.getChildren() != null) {
      // Process WHEN conditions
      for (int i = 0; i < expr.getChildren().size() - 1; i += 2) {
        // Add CONDITIONAL for the WHEN part
        traverseExpression(
            outputCLL,
            outputColumn,
            expr.getChildren().get(i),
            transformationInfo.merge(
                TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL)));
        // Pass on the transformation as-is for the THEN part
        traverseExpression(
            outputCLL, outputColumn, expr.getChildren().get(i + 1), transformationInfo);
      }
      // Process ELSE condition if present
      if (expr.getChildren().size() % 2 == 1) {
        traverseExpression(
            outputCLL,
            outputColumn,
            expr.getChildren().get(expr.getChildren().size() - 1),
            transformationInfo);
      }
    }
  }

  static void handleIfExpression(
      OutputCLL outputCLL,
      String outputColumn,
      IfExpr expr,
      TransformationInfo transformationInfo) {
    if (expr.getChildren() != null) {
      // Process the condition
      traverseExpression(
          outputCLL,
          outputColumn,
          expr.getChildren().get(0),
          transformationInfo.merge(
              TransformationInfo.indirect(TransformationInfo.Subtypes.CONDITIONAL)));
      // Process the 'then'
      traverseExpression(outputCLL, outputColumn, expr.getChildren().get(1), transformationInfo);
      // Process the 'else'
      traverseExpression(outputCLL, outputColumn, expr.getChildren().get(2), transformationInfo);
    }
  }

  static void handleGenericExpression(
      OutputCLL outputCLL,
      String outputColumn,
      GenericExpr expr,
      TransformationInfo transformationInfo) {
    if (expr.getChildren() != null) {
      for (BaseExpr child : expr.getChildren()) {
        traverseExpression(
            outputCLL,
            outputColumn,
            child,
            transformationInfo.merge(TransformationInfo.transformation()));
      }
    }
  }

  static boolean isMasking(AggregateExpr agg) {
    if (agg.getFunction() instanceof UDFAdditionalLineage) {
      return ((UDFAdditionalLineage) agg.getFunction()).isMasking();
    }
    return isMasking(agg.getFunction().getClass());
  }

  static boolean isMasking(UDTFExpr udtf) {
    if (udtf.getFunction() instanceof UDFAdditionalLineage) {
      return ((UDFAdditionalLineage) udtf.getFunction()).isMasking();
    }
    return isMasking(udtf.getFunction().getClass());
  }

  static boolean isMasking(FunctionExpr func) {

    if (func.getFunction() == null) {
      return false;
    } else if (func.getFunction() instanceof GenericUDFBridge) {
      return isMasking(((GenericUDFBridge) func.getFunction()).getUdfClass());
    } else if (func.getFunction() instanceof UDFAdditionalLineage) {
      return ((UDFAdditionalLineage) func.getFunction()).isMasking();
    } else {
      return isMasking(func.getFunction().getClass());
    }
  }

  static boolean isMasking(Class<?> klass) {
    return MASKING_CLASSES.stream().anyMatch(c -> c.equals(klass));
  }
}
