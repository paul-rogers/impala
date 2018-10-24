// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.rewrite;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Rewrites conditional functions to use a CASE statement.
 * The conditional functions vanish from the plan after this
 * rewrite: there is no back-end implementation for these functions.
 * <ul>
 * <li><code>coalesce(v1, v2, ...)</code></li>
 * <li><code>if(condition, ifTrue, ifFalseOrNull)</code></li>
 * <li><code>ifnull(a, ifNull)</code></li>
 * <li><code>isnull(a, ifNull)</code></li>
 * <li><code>nullif(expr1, expr2)</code></</li>
 * <li><code>nvl(a, ifNull)</code></li>
 * </ul>
 * <p>
 * Since every function is rewritten to a <code>CASE</code>
 * statement, the planner runs the rule to simplify <code>CASE</code>
 * after this rule. Where that other rule can perform simplications,
 * those simplifications are omitted here. However, the <code>CASE</code>
 * case rules are limited (See IMPALA-7750), so several optimizations
 * appear here that can be removed once IMPALA-7750 is fixed.
 */
public class RewriteConditionalFnsRule  implements ExprRewriteRule {
  public static RewriteConditionalFnsRule INSTANCE = new RewriteConditionalFnsRule();

  private RewriteConditionalFnsRule() { }

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!expr.isAnalyzed()) return expr;

    // Rewrite conditional functions to use CASE. The result becomes
    // the original expression since there is no implementation for the
    // rewritten function. All rewritten functions use CASE, so we'll
    // then want to allow CASE to do any simplification (reverting to
    // the original case expression if we don't pass the aggregate
    // test below.
    if (expr instanceof FunctionCallExpr) {
      expr = rewriteConditionalFn((FunctionCallExpr) expr);
      expr.analyze(analyzer);
    }
    return expr;
  }

  /**
   * Transform SQL functions that require short-circuit evaluation
   * into the equivalent <code>CASE</code> expressions. The BE does
   * code gen for <CASE>, avoiding the need for ad-hoc implementations
   * of each function.
   */
  private Expr rewriteConditionalFn(FunctionCallExpr expr) {
    switch (expr.getFnName().getFunction()) {
    case "if":
      return rewriteIfFn(expr);
    case "coalesce":
      return rewriteCoalesceFn(expr);
    case "isnull":
    case "nvl":
    case "ifnull":
      return rewriteIfNullFn(expr);
    case "nvl2":
      return rewriteNvl2Fn(expr);
    case "nullif":
      return rewriteNullIfFn(expr);
    default:
      return expr;
    }
  }

  /**
   * Rewrites <code>IF(cond, thenExpr, elseExpr)</code>  &rarr;
   * <code>CASE WHEN cond THEN thenExpr ELSE elseExpr END</code>.
   * <p>
   * Relies on <code>CASE</code> simplification to perform the
   * following simplifications:
   * <ul>
   * <li><code>IF(TRUE, then, else)</code> &rarr; <code>then</code></li>
   * <li><code>IF(FALSE|NULL, then, else)</code> &rarr; <code>else</code></li>
   * </ul>
   */
  private Expr rewriteIfFn(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 3);
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
            new CaseWhenClause( // WHEN cond THEN thenExpr
                expr.getChild(0),
                expr.getChild(1))),
        expr.getChild(2)); // ELSE elseExpr END
  }

  /**
   * Rewrites <code>IFNULL(a, x)</code>:
   * <ul>
   * <li><code>IFNULL(NULL, x)</code> &rarr; <code>x</code></li>
   * <li><code>IFNULL(a, x)</code> &rarr; <code>a</code>, if a is a non-null literal</li>
   * <li><code>IFNULL(a, x)</code> &rarr; <br>
   * <code>CASE WHEN a IS NULL THEN x ELSE a END</code></li>
   * </ul>
   */
  private Expr rewriteIfNullFn(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 2);
    Expr child0 = expr.getChild(0);
    Expr child1 = expr.getChild(1);
    if (child0.isNullLiteral()) return child1;
    if (child0.isLiteral() && ! child1.isAggregate()) return child0;

    // Transform into a CASE expression
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
            new CaseWhenClause( // WHEN a IS NULL
                new IsNullPredicate(child0, false),
                child1)), // THEN x
        child0.clone()); // ELSE a END
  }

  /**
   * Rewrites <code>COALESCE(a, b, ...)</code> by skipping nulls and
   * applying the following transformations:
   * <ul>
   * <li><code>COALESCE(null, a, b)</code> &rarr; <code>COALESCE(a, b)</code></li>
   * <li><code>COALESCE(a, null, c)</code> &rarr; <code>COALESCE(a, c)</code></li>
   * <li><code>COALESCE(<i>literal</i>, a, b)</code> &rarr;
   * <code><i>literal</i></code>, when literal is not NullLiteral</code></li>
   * <li><code>COALESCE(a, <i>literal</i>, b)</code> &rarr;
   * <code>COALESCE(a, <i>literal</i>)</code> when literal is not NullLiteral;</li>
   * <li><code>COALESCE(a, b)</code> &rarr; <br>
   * <code>CASE WHEN a IS NOT NULL THEN a <br>
   * WHEN b IS NOT NULL THEN b END</code></li>
   * </ul>
   * A special case occurs if the resulting rules remove all
   * aggregate functions. If so, the rewrite must be done again to include
   * at least one aggregate, even if that aggregate won't ever be evaluated.
   */
  private Expr rewriteCoalesceFn(FunctionCallExpr expr) {
    boolean hasAgg = expr.contains(Expr.isAggregatePredicate());
    List<Expr> revised = new ArrayList<>();
    boolean sawAgg = false;
    for (Expr childExpr : expr.getChildren()) {
      // Skip nulls.
      if (childExpr.isNullLiteral()) continue;
      revised.add(childExpr);
      if (childExpr.isLiteral() &&
          (! hasAgg || sawAgg)) break;
      if (childExpr.isAggregate()) { sawAgg = true; }
    }
    if (revised.isEmpty()) { return NullLiteral.create(expr.getType()); }
    if (revised.size() == 1) { return revised.get(0); }

    List<CaseWhenClause> whenList = new ArrayList<>();
    for (int i = 0; i < revised.size() - 1; i++) {
      Expr childExpr = revised.get(i);
      whenList.add(new CaseWhenClause(
          new IsNullPredicate(childExpr, true), childExpr.clone()));
    }
    return new CaseExpr(null, whenList,
        revised.get(revised.size() - 1));
  }

  /**
   * Rewrite of <code>nvl2(expr, ifNotNull, ifNull)</code>.
   * <ul>
   * <li><code>nvl2(null, x, y)</code> &rarr; <code>y</code></li>
   * <li><code>nvl2(non_null_literal, x, y)</code> &rarr; <code>x</code></li>
   * <li><code>nvl2(expr, x y)</code> &rarr; <br>
   * <code>CASE WHEN expr IS NOT NULL THEN x ELSE y END</code></li>
   * <ul>
   * Apply optimizations only if does not drop an aggregate.
   */
  private Expr rewriteNvl2Fn(FunctionCallExpr expr) {
    List<Expr> plist = expr.getParams().exprs();
    Expr head = plist.get(0);
    Expr ifNotNull = plist.get(1);
    Expr ifNull = plist.get(2);
    if (head.isNullLiteral() &&
        (ifNull.isAggregate() || ! ifNotNull.isAggregate())) {
      return ifNull;
    }
    if (head.isLiteral() &&
        (ifNotNull.isAggregate() || ! ifNull.isAggregate())) {
      return ifNotNull;
    }
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
          new CaseWhenClause( // WHEN
            new IsNullPredicate(head, true), // EXPR IS NOT NULL
            ifNotNull)), // THEN ifNotNull
        ifNull); // ELSE isNull END
  }

  /**
   * Rewrite of <code>nullif(x, y)</code>.
   * <ul>
   * <li><code>nullif(null, y)</code> &rarr; <code>NULL</code></li>
   * <li><code>nullif(x, null)</code> &rarr; <code>NULL</code></li>
   * <li><code>nullif(x y)</code> &rarr; <br>
   * <code>CASE WHEN x IS DISTINCT FROM y THEN x END</code></li>
   * <ul>
   */
  private Expr rewriteNullIfFn(FunctionCallExpr expr) {
    List<Expr> plist = expr.getParams().exprs();
    Expr head = plist.get(0);
    if (head.isNullLiteral()) { return head; }
    Expr tail = plist.get(1);
    if (tail.isNullLiteral()) { return tail; }
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
          new CaseWhenClause( // WHEN
            new BinaryPredicate(BinaryPredicate.Operator.DISTINCT_FROM, head,
                tail), // x IS DISTINCT FROM y
            head.clone())), // THEN x
        null); // END (which defaults to NULL)
  }
}
