/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.TypedFilter;

/**
 * Extracts expression dependencies from a {@link TypedFilter} operator in {@link LogicalPlan} — the
 * node produced by the typed {@code Dataset.filter(T => Boolean)} / {@code filter(FilterFunction)}
 * overloads, as opposed to the untyped {@code filter(Column)} which produces a {@link
 * org.apache.spark.sql.catalyst.plans.logical.Filter} and is handled by {@link FilterVisitor}.
 *
 * <pre>{@code
 * people.filter(p -> p.getAge() > 20)   // TypedFilter
 * people.filter(col("age").gt(20))      // Filter
 * }</pre>
 *
 * <p>A {@code TypedFilter} passes its child's attributes through unchanged, so {@code output()} and
 * {@code child().output()} share {@code exprId}s and the column-lineage facet is already populated
 * edge-free with identity fields. The gap this visitor closes is narrower: without it, a typed
 * filter looks like a plain pass-through rather than a row-set restriction, because the {@code
 * INDIRECT/FILTER} edge that the untyped path contributes is missing.
 *
 * <h2>Known limitation: the edge set is BROADER than the untyped equivalent</h2>
 *
 * <p>The predicate itself is opaque JVM bytecode; it is not in the plan. The only structural handle
 * is {@code deserializer()}, whose {@code references()} are real {@link
 * org.apache.spark.sql.catalyst.expressions.AttributeReference}s into the child — but they are the
 * <b>whole deserialized schema, not the columns the predicate actually read</b>, because the
 * deserializer materialises the entire object before the lambda runs. Measured on Spark 4.0.0: for
 * {@code p -> p.getAge() > 20} over a {@code Person} bean with {@code name, email, age}, {@code
 * deserializer().references()} is {@code [age, email, name]} and {@code argumentSchema} is {@code
 * struct<age:int,email:string,name:string>} — all three columns, although the lambda reads only
 * {@code age}.
 *
 * <p>So the claim this visitor emits is "this filter read some subset of these columns", which
 * over-claims breadth relative to the untyped path's exact predicate columns. It is strictly better
 * than today's silence about filtering and it never asserts a false <em>specific</em> pairing, but
 * it is <b>not</b> parity with {@link FilterVisitor}. There is no plan-level way to narrow it; that
 * would require bytecode analysis of the lambda.
 *
 * <p>Implementation note: the deserializer is handed to {@link ExpressionTraverser} rather than
 * having its {@code references()} enumerated, so this takes exactly the shape of {@link
 * FilterVisitor} and reaches the same attribute set (the leaf {@code AttributeReference}s of the
 * tree). That also sidesteps an ordering hazard: {@code references()} is an unordered {@code
 * AttributeSet} which iterates {@code [c0, c1, c10, c11, c2, …]} on a 12-column table, i.e.
 * lexicographically rather than in schema order. It is safe as an unordered <em>set</em> for filter
 * breadth, as used here, but must never be used for positional matching — {@code
 * DeserializeToObject.child().output()} is the schema-ordered accessor for that.
 */
public class TypedFilterVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof TypedFilter;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    TypedFilter typedFilter = (TypedFilter) operator;
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    ExpressionTraverser.of(
            typedFilter.deserializer(), exprId, TransformationInfo.indirect(FILTER), builder)
        .traverse();
  }
}
