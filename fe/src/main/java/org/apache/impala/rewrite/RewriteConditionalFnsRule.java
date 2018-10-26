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
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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

  private static List<String> CONDITIONAL_FNS = ImmutableList.of(
      "if", "coalesce", "isnull", "nvl", "ifnull", "nvl2", "nullif" );

  public static boolean isRewrittenFunction(Expr expr) {
    if (! (expr instanceof FunctionCallExpr)) { return false; }
    return CONDITIONAL_FNS.contains(((FunctionCallExpr) expr).getFnName().getFunction());
  }

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    assert expr.isAnalyzed();

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
    case "nvl2":
      return rewriteNvl2Fn(expr);
    case "nullif":
      return rewriteNullIfFn(expr);
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

  /**
   * Rewrite of nvl2(expr, ifNotNull, ifNull).
   *
   * nvl2(null, x, y) --> y
   * nvl2(non_null_literal, x, y) --> x
   * nvl2(expr, x y) --> <br>
   * CASE WHEN expr IS NOT NULL THEN x ELSE y END
   *
   * Apply optimizations only if does not drop an aggregate.
   */
  private Expr rewriteNvl2Fn(FunctionCallExpr expr) {
    List<Expr> plist = expr.getParams().exprs();
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
          new CaseWhenClause( // WHEN
            new IsNullPredicate(plist.get(0), true), // EXPR IS NOT NULL
            plist.get(1))), // THEN ifNotNull
        plist.get(2)); // ELSE ifNull END
  }

  /**
   * Rewrite of nullif(x, y).
   *
   * nullif(x y) -->
   *   CASE WHEN x IS DISTINCT FROM y THEN x END
   * nullif(null, y) --> NULL
   * nullif(x, null) --> x
   *
   * Handles simplifications here because they benefit from
   * semantic knowledge of the function.
   */
  private Expr rewriteNullIfFn(FunctionCallExpr expr) {
    List<Expr> plist = expr.getParams().exprs();
    Expr head = plist.get(0);

    // Nothing is equal to null, so return the head.
    if (head.isNullLiteral()) { return head; }
    Expr tail = plist.get(1);
    if (tail.isNullLiteral()) { return head; }

    // Full rewrite to CASE.
    return new CaseExpr(null, // CASE
        Lists.newArrayList(
          new CaseWhenClause( // WHEN
            new BinaryPredicate(BinaryPredicate.Operator.DISTINCT_FROM, head,
                tail), // x IS DISTINCT FROM y
            head.clone())), // THEN x
        null); // END (which defaults to NULL)
  }

  /**
   * Create entries for the odd-duck NVL2 function:
   * type1 nvl2(type2 expr, type1 ifNotNull, type1 ifNull).
   * The types form an n^2 matrix that can't easily be represented.
   * Instead, we define a special function that matches on the
   * second and third arguments, since they determine the return
   * type. We then ignore the first argument since we only care if
   * it is null, and CASE will take care of the details.
   */
  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      if (t.isScalarType(PrimitiveType.CHAR)) continue;
      db.addBuiltin(new ScalarFunction.Nvl2Function(t));
    }
  }
}
