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
 *
 * coalesce(v1, v2, ...)
 * if(condition, ifTrue, ifFalseOrNull)
 * ifnull(a, ifNull)
 * isnull(a, ifNull)
 * nullif(expr1, expr2)
 * nvl(a, ifNull)
 *
 * Since every function is rewritten to a CASE
 * statement, the planner runs the rule to simplify CASE
 * after this rule. Where that other rule can perform simplifications,
 * those simplifications are omitted here. However, the CASE
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
      Expr revised =  rewriteConditionalFn((FunctionCallExpr) expr);

      // Workaround for IMALA-TBD
      // Rewrite engine does not re-analyze rewritten rules, each rule
      // must do this in an ad-hoc manner. Rewrite is needed
      // so that case simplification rules will fire.
      if (revised != expr) {
        revised.analyze(analyzer);
        expr = revised;
      }
    }
    return expr;
  }

  /**
   * Transform SQL functions that require short-circuit evaluation
   * into the equivalent CASE expressions. The BE does
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
    default:
      return expr;
    }
  }

  /**
   * Rewrites IF(cond, thenExpr, elseExpr)  -->
   * CASE WHEN cond THEN thenExpr ELSE elseExpr END.
   *
   * Relies on CASE simplification to perform the
   * following simplifications:
   *
   * IF(TRUE, thenExpr, elseExpr) --> thenExpr
   * IF(FALSE|NULL, thenExpr, elseExpr) --> elseExpr
   *
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
   * Rewrites IFNULL(a, x), which is an alias
   * for ISNULL(a, x) and NVL(a, x).
   *
   * IFNULL(NULL, x) --> x
   * IFNULL(a, x) --> a, if a is a non-null literal
   * IFNULL(a, x) --> <br>
   * CASE WHEN a IS NULL THEN x ELSE a END
   */
  private Expr rewriteIfNullFn(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 2);
    Expr child0 = expr.getChild(0);
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
            new CaseWhenClause( // WHEN a IS NULL
                new IsNullPredicate(child0, false),
                expr.getChild(1))), // THEN x
        child0.clone()); // ELSE a END
  }

  /**
   * Rewrites COALESCE(a, b, ...) by skipping nulls and
   * applying the following transformations:
   *
   * COALESCE(null, a, b) --> COALESCE(a, b)
   * COALESCE(a, null, c) --> COALESCE(a, c)
   * COALESCE(literal, a, b) -->
   * literal, when literal is not NullLiteral
   * COALESCE(a, literal, b) -->
   * COALESCE(a, literal) when literal is not NullLiteral;
   * COALESCE(a, b) --> <br>
   * CASE WHEN a IS NOT NULL THEN a <br>
   * WHEN b IS NOT NULL THEN b END
   *
   * A special case occurs if the resulting rules remove all
   * aggregate functions. If so, the rewrite must be done again to include
   * at least one aggregate, even if that aggregate won't ever be evaluated.
   * See IMPALA-5125.
   *
   * The simplifications are done here because they benefit from knowledge
   * of the semantics of COALESCE(), and are difficult to do once encoded
   * as a CASE statement.
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

    // Iterate over all values but the last to put these
    // into WHERE clauses. The last value goes into the ELSE
    // clause.
    List<CaseWhenClause> whenList = new ArrayList<>();
    for (int i = 0; i < revised.size() - 1; i++) {
      Expr childExpr = revised.get(i);
      whenList.add(new CaseWhenClause(
          new IsNullPredicate(childExpr, true), childExpr.clone()));
    }
    return new CaseExpr(null, whenList,
        revised.get(revised.size() - 1));
  }
}
