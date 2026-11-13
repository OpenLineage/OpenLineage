/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static io.openlineage.flink.sqlagent.PlannerReflection.*;

import java.util.*;

/** Rules over resolved relational nodes and Rex expressions, never over SQL text. */
final class SqlColumnLineage {
  interface OriginReference {
    Map<String, Object> resolve(Object origin) throws Exception;
  }

  private final Object metadata;
  private final OriginReference reference;
  private final Map<Object, Map<Integer, ColumnDependencies>> cache = new IdentityHashMap<>();

  SqlColumnLineage(Object metadata, OriginReference reference) {
    this.metadata = metadata;
    this.reference = reference;
  }

  ColumnDependencies field(Object rel, int index) throws Exception {
    Map<Integer, ColumnDependencies> fields =
        cache.computeIfAbsent(rel, ignored -> new HashMap<>());
    if (fields.containsKey(index)) return fields.get(index);
    ColumnDependencies result = resolveField(rel, index);
    fields.put(index, result);
    return result;
  }

  private ColumnDependencies resolveField(Object rel, int index) throws Exception {
    List<?> children = (List<?>) call(rel, "getInputs");
    String kind = call(rel, "getRelTypeName").toString();
    if (has(rel, "getProjects"))
      return expression(children.get(0), ((List<?>) call(rel, "getProjects")).get(index));
    if (has(rel, "getProgram")) {
      Object program = call(rel, "getProgram");
      Object expr =
          call(program, "expandLocalRef", ((List<?>) call(program, "getProjectList")).get(index));
      return expression(children.get(0), expr);
    }
    if (has(rel, "getAggCallList")) {
      List<?> keys = (List<?>) call(call(rel, "getGroupSet"), "asList");
      if (index < keys.size()) return field(children.get(0), (Integer) keys.get(index));
      Object aggregate = ((List<?>) call(rel, "getAggCallList")).get(index - keys.size());
      ColumnDependencies result = new ColumnDependencies();
      for (Object arg : (List<?>) call(aggregate, "getArgList"))
        result.merge(field(children.get(0), (Integer) arg));
      result = result.transform("DIRECT", "AGGREGATION", aggregate.toString());
      int filter = (Integer) member(aggregate, "filterArg");
      if (filter >= 0)
        result.merge(
            field(children.get(0), filter).transform("INDIRECT", "FILTER", aggregate.toString()));
      for (Object collation :
          (List<?>) call(call(aggregate, "getCollation"), "getFieldCollations")) {
        result.merge(
            field(children.get(0), (Integer) call(collation, "getFieldIndex"))
                .transform("INDIRECT", "SORT", aggregate.toString()));
      }
      return result;
    }
    if (kind.contains("Join")) {
      int leftCount = width(children.get(0));
      return index < leftCount
          ? field(children.get(0), index)
          : field(children.get(1), index - leftCount);
    }
    if (kind.contains("Union") || kind.contains("Intersect") || kind.contains("Minus")) {
      ColumnDependencies result = new ColumnDependencies();
      for (Object child : children) result.merge(field(child, index));
      return result;
    }
    if (children.size() == 1 && (kind.contains("Filter") || kind.contains("Sort")))
      return field(children.get(0), index);
    // Other planner nodes retain Calcite's origins, with no invented expression semantics.
    Set<?> origins = (Set<?>) call(metadata, "getColumnOrigins", rel, index);
    if (origins == null)
      throw new IllegalStateException(
          "Planner cannot resolve origins for " + kind + " output " + index);
    ColumnDependencies result = new ColumnDependencies();
    for (Object origin : origins) {
      boolean derived = (Boolean) call(origin, "isDerived");
      result.add(
          reference.resolve(origin), "DIRECT", derived ? "TRANSFORMATION" : "IDENTITY", null);
    }
    return result;
  }

  private ColumnDependencies expression(Object input, Object expr) throws Exception {
    String kind = call(expr, "getKind").toString();
    if (kind.equals("INPUT_REF")) return field(input, (Integer) call(expr, "getIndex"));
    if (kind.equals("LITERAL")) return new ColumnDependencies();
    ColumnDependencies result = new ColumnDependencies();
    if (kind.equals("CASE")) {
      List<?> operands = (List<?>) call(expr, "getOperands");
      for (int i = 0; i < operands.size(); i++) {
        boolean condition = i < operands.size() - 1 && i % 2 == 0;
        result.merge(
            expression(input, operands.get(i))
                .transform(
                    condition ? "INDIRECT" : "DIRECT",
                    condition ? "CONDITIONAL" : "TRANSFORMATION",
                    expr.toString()));
      }
      return result;
    }
    if (has(expr, "getWindow")) {
      for (Object operand : (List<?>) call(expr, "getOperands"))
        result.merge(expression(input, operand));
      result = result.transform("DIRECT", "AGGREGATION", expr.toString());
      Object window = call(expr, "getWindow");
      for (Object key : (List<?>) member(window, "partitionKeys")) {
        result.merge(expression(input, key).transform("INDIRECT", "WINDOW", window.toString()));
      }
      for (Object key : (List<?>) member(window, "orderKeys")) {
        result.merge(
            expression(input, member(key, "left"))
                .transform("INDIRECT", "WINDOW", window.toString()));
      }
      return result;
    }
    if (has(expr, "getOperands")) {
      for (Object operand : (List<?>) call(expr, "getOperands"))
        result.merge(expression(input, operand));
    } else if (has(expr, "getReferenceExpr")) {
      result.merge(expression(input, call(expr, "getReferenceExpr")));
    } else {
      throw new IllegalStateException(
          "Unsupported planner expression: " + expr.getClass().getName());
    }
    return result.transform("DIRECT", "TRANSFORMATION", expr.toString());
  }

  ColumnDependencies dataset(Object root) throws Exception {
    ColumnDependencies result = new ColumnDependencies();
    collect(root, result, Collections.newSetFromMap(new IdentityHashMap<>()));
    return result;
  }

  private void collect(Object rel, ColumnDependencies result, Set<Object> visited)
      throws Exception {
    if (!visited.add(rel)) return;
    List<?> children = (List<?>) call(rel, "getInputs");
    String kind = call(rel, "getRelTypeName").toString();
    if (kind.contains("Join") && has(rel, "getCondition")) {
      // Join condition input ordinals address the concatenated left and right row.
      result.merge(
          expression(rel, call(rel, "getCondition"))
              .transform("INDIRECT", "JOIN", call(rel, "getCondition").toString()));
    } else if (kind.contains("Filter") && has(rel, "getCondition")) {
      result.merge(
          expression(children.get(0), call(rel, "getCondition"))
              .transform("INDIRECT", "FILTER", call(rel, "getCondition").toString()));
    }
    if (has(rel, "getProgram")) {
      Object program = call(rel, "getProgram");
      Object condition = call(program, "getCondition");
      if (condition != null) {
        Object expanded = call(program, "expandLocalRef", condition);
        result.merge(
            expression(children.get(0), expanded)
                .transform("INDIRECT", "FILTER", expanded.toString()));
      }
    }
    if (has(rel, "getAggCallList")) {
      for (Object key : (List<?>) call(call(rel, "getGroupSet"), "asList")) {
        result.merge(field(children.get(0), (Integer) key).transform("INDIRECT", "GROUP_BY", null));
      }
    }
    if (kind.contains("Sort") && has(rel, "getCollation")) {
      for (Object collation : (List<?>) call(call(rel, "getCollation"), "getFieldCollations")) {
        int index = (Integer) call(collation, "getFieldIndex");
        result.merge(
            field(children.get(0), index).transform("INDIRECT", "SORT", collation.toString()));
      }
    }
    for (Object child : children) collect(child, result, visited);
  }

  private int width(Object rel) throws Exception {
    return ((List<?>) call(call(rel, "getRowType"), "getFieldList")).size();
  }
}
