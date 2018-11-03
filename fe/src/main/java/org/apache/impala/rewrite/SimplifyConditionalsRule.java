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
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CaseWhenClause;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.Predicate;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/***
 * This rule simplifies conditional functions with constant conditions. It relies on
 * FoldConstantsRule to replace the constant conditions with a BoolLiteral or NullLiteral
 * first, and on NormalizeExprsRule to normalize CompoundPredicates.
 *
 * Examples:
 * if (true, 0, 1) -> 0
 * id = 0 OR false -> id = 0
 * false AND id = 1 -> false
 * case when false then 0 when true then 1 end -> 1
 * coalesce(1, 0) -> 1
 *
 * Unary functions like isfalse, isnotfalse, istrue, isnottrue, nullvalue,
 * and nonnullvalue don't need special handling as the fold constants rule
 * will handle them.  nullif and nvl2 are converted to an if in FunctionCallExpr,
 * and therefore don't need handling here.
 */
public class SimplifyConditionalsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyConditionalsRule();

  private static List<String> IFNULL_ALIASES = ImmutableList.of(
      "ifnull", "isnull", "nvl");

  private SimplifyConditionalsRule() { }

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!expr.isAnalyzed()) return expr;

    Expr simplified;
    if (expr instanceof FunctionCallExpr) {
      simplified = simplifyFunctionCallExpr((FunctionCallExpr) expr);
    } else if (expr instanceof CompoundPredicate) {
      simplified = simplifyCompoundPredicate((CompoundPredicate) expr);
    } else if (expr instanceof CaseExpr) {
      simplified = simplifyCaseExpr((CaseExpr) expr, analyzer);
    } else {
      return expr;
    }

    // IMPALA-5125: We can't eliminate aggregates as this may change the meaning of the
    // query, for example:
    // 'select if (true, 0, sum(id)) from alltypes' != 'select 0 from alltypes'
    if (expr != simplified) {
      simplified.analyze(analyzer);
      if (expr.contains(Expr.isAggregatePredicate())
          && !simplified.contains(Expr.isAggregatePredicate())) {
        return expr;
      }
    }
    return simplified;
  }

  /**
   * Simplifies IF by returning the corresponding child if the
   * condition has a constant
   * TRUE, FALSE, or NULL (equivalent to FALSE) value.
   */
  private Expr simplifyIfFunctionCallExpr(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 3);
    Expr head = expr.getChild(0);
    if (Predicate.IS_TRUE_LITERAL.apply(head)) {
      // IF(TRUE)
      return expr.getChild(1);
    } else if (Predicate.IS_FALSE_LITERAL.apply(head)) {
      // IF(FALSE)
      return expr.getChild(2);
    } else if (Expr.IS_NULL_VALUE.apply(head)) {
      // IF(NULL)
      // IF(CAST(NULL AS <type), ...)
      return expr.getChild(2);
    }
    return expr;
  }

  /**
   * Simplifies IFNULL, ISNULL, NVL if the condition is a literal, using the
   * following transformations:
   *   IFNULL(NULL, x) -> x
   *   IFNULL(a, x) -> a, if a is a non-null literal
   */
  private Expr simplifyIfNullFunctionCallExpr(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 2);
    Expr child0 = expr.getChild(0);
    if (Expr.IS_NULL_VALUE.apply(child0)) return expr.getChild(1);
    if (Expr.IS_LITERAL.apply(child0)) return child0;
    return expr;
  }

  /**
   * Simplify COALESCE by skipping leading nulls and applying the
   * following transformations:
   *
   * COALESCE(null, a, b) -> COALESCE(a, b);
   * COALESCE(<literal>, a, b) -> <literal>, when literal is not NullLiteral;
   */
  private Expr simplifyCoalesceFunctionCallExpr(FunctionCallExpr expr) {
    int numChildren = expr.getChildren().size();
    Expr result = NullLiteral.create(expr.getType());
    for (int i = 0; i < numChildren; ++i) {
      Expr childExpr = expr.getChildren().get(i);
      // Skip leading nulls.
      if (Expr.IS_NULL_VALUE.apply(childExpr)) continue;
      if ((i == numChildren - 1) || Expr.IS_LITERAL.apply(childExpr)) {
        result = childExpr;
      } else if (i == 0) {
        result = expr;
      } else {
        List<Expr> newChildren = Lists.newArrayList(
            expr.getChildren().subList(i, numChildren));
        result = new FunctionCallExpr(expr.getFnName(), newChildren);
      }
      break;
    }
    return result;
  }

  private Expr simplifyFunctionCallExpr(FunctionCallExpr expr) {
    String fnName = expr.getFnName().getFunction();

    if (fnName.equals("if")) {
      return simplifyIfFunctionCallExpr(expr);
    } else if (fnName.equals("coalesce")) {
      return simplifyCoalesceFunctionCallExpr(expr);
    } else if (IFNULL_ALIASES.contains(fnName)) {
      return simplifyIfNullFunctionCallExpr(expr);
    }
    return expr;
  }

  /**
   * Simplifies compound predicates with at least one BoolLiteral child, which
   * NormalizeExprsRule ensures will be the left child,  according to the
   * following rules:
   *
   * true AND 'expr' -> 'expr'
   * false AND 'expr' -> false
   * true OR 'expr' -> true
   * false OR 'expr' -> 'expr'
   *
   * Unlike other rules here such as IF, we cannot in general simplify CompoundPredicates
   * with a NullLiteral child (unless the other child is a BoolLiteral), eg. null and
   * 'expr' is false if 'expr' is false but null if 'expr' is true.
   *
   * NOT is covered by FoldConstantRule.
   */
  private Expr simplifyCompoundPredicate(CompoundPredicate expr) {
    Expr leftChild = expr.getChild(0);
    if (!(leftChild instanceof BoolLiteral)) return expr;

    if (expr.getOp() == CompoundPredicate.Operator.AND) {
      if (Predicate.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE AND 'expr', so return 'expr'.
        return expr.getChild(1);
      } else if (Predicate.IS_FALSE_LITERAL.apply(leftChild)) {
        // FALSE AND 'expr', so return FALSE.
        return leftChild;
      }
    } else if (expr.getOp() == CompoundPredicate.Operator.OR) {
      if (Predicate.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE OR 'expr', so return TRUE.
        return leftChild;
      } else if (Predicate.IS_FALSE_LITERAL.apply(leftChild)) {
        // FALSE OR 'expr', so return 'expr'.
        return expr.getChild(1);
      }
    }
    return expr;
  }

  /**
   * Simplifies CASE and DECODE. If any of the 'when's have constant FALSE/NULL values,
   * they are removed. If all of the 'when's are removed, just the ELSE is returned. If
   * any of the 'when's have constant TRUE values, the leftmost one becomes the ELSE
   * clause and all following cases are removed.
   *
   * If no ELSE clause exists (or it is NULL), then drops any tail WHEN clauses that
   * have NULL as their THEN value. (But, only does this if some other simplification
   * is done.)
   */
  private Expr simplifyCaseExpr(CaseExpr expr, Analyzer analyzer)
      throws AnalysisException {

    if (expr.hasCaseExpr()) {
      return simplifyCaseWithExpr(expr, analyzer);
    } else {
      return simplifyCaseWithoutExpr(expr);
    }
  }

  /**
   * Simplify CASE expr WHEN ... END.
   *
   * CASE null WHEN ... END --> NULL
   * CASE const WHEN ... WHEN const THEN value ... END -->
   *   CASE const WHEN ... ELSE const END
   * CASE expr ... WHEN null ... END -->
   *   CASE expr ... END
   */
  private Expr simplifyCaseWithExpr(CaseExpr expr, Analyzer analyzer)
      throws AnalysisException {
    Expr caseExpr = expr.getCaseExpr();
    Expr elseExpr = expr.getElseExpr();

    // CASE null WHEN ... ELSE ... END
    // Return the ELSE clause or NULL.
    if (Expr.IS_NULL_VALUE.apply(caseExpr)) {
      return elseExpr == null
          ? NullLiteral.create(expr.getType())
          : elseExpr;
    }

    boolean isConstantCaseExpr = Expr.IS_LITERAL.apply(caseExpr);
    int numChildren = expr.getChildCount();
    int loopStart = expr.hasCaseExpr() ? 1 : 0;

    // Check and return early if there's nothing that can be simplified.
    boolean canSimplify = false;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr child = expr.getChild(i);
      if (Expr.IS_NULL_VALUE.apply(child)) {
        canSimplify = true;
        break;
      }
      if (isConstantCaseExpr && Expr.IS_LITERAL.apply(child)) {
        canSimplify = true;
        break;
      }
    }

    // Remove trivial ELSE
    canSimplify |= (elseExpr != null &&  Expr.IS_NULL_VALUE.apply(elseExpr));
    if (! canSimplify) { return expr; }

    // Simplification is possible.
    // To prevent an infinite loop, after this rewrite,
    // the above check should not pass.

    List<CaseWhenClause> newWhenClauses = new ArrayList<>();
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr whenExpr = expr.getChild(i);
      Expr thenValue = expr.getChild(i + 1);

      if (Expr.IS_NULL_VALUE.apply(whenExpr)) {
        // Null never matches the expression
      } else if (isConstantCaseExpr && Expr.IS_LITERAL.apply(whenExpr)) {
        // CONSTANT1 = CONSTANT2
        BinaryPredicate pred = new BinaryPredicate(
            BinaryPredicate.Operator.EQ, caseExpr, expr.getChild(i));
        pred.analyze(analyzer);
        Expr result = analyzer.getConstantFolder().rewrite(pred, analyzer);
        if (Predicate.IS_TRUE_LITERAL.apply(result)) {
          // The terms match: caseExpr == whenExpr
          elseExpr = expr.getChild(i + 1);
          break;
        }
        // Ignore caseExpr != whenExpr
      } else {
        // Retain all other terms
        newWhenClauses.add(new CaseWhenClause(whenExpr, thenValue));
      }
    }

    return buildCase(expr, caseExpr, newWhenClauses, elseExpr);
  }

  /**
   * Simplify CASE WHEN bool-expr THEN value ... END
   *
   * CASE ... WHEN false THEN value ... END -->
   *   CASE ... END
   * CASE ... WHEN false THEN value ... END -->
   *   CASE ... END
   * CASE ... WHEN null THEN value ... END -->
   *   CASE ... ... END
   */
  private Expr simplifyCaseWithoutExpr(CaseExpr expr)
      throws AnalysisException {

    Expr elseExpr = expr.getElseExpr();
    int numChildren = expr.getChildCount();
    int loopStart = expr.hasCaseExpr() ? 1 : 0;

    // Check and return early if there's nothing that can be simplified.
    boolean canSimplify = false;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr child = expr.getChild(i);
      if (Expr.IS_LITERAL.apply(child)) {
        canSimplify = true;
        break;
      }
    }

    // Remove trivial ELSE
    canSimplify |= (elseExpr != null &&  Expr.IS_NULL_VALUE.apply(elseExpr));
    if (! canSimplify) { return expr; }

    List<CaseWhenClause> newWhenClauses = new ArrayList<>();
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr whenExpr = expr.getChild(i);
      Expr thenValue = expr.getChild(i + 1);

      if (Expr.IS_NULL_VALUE.apply(whenExpr)) {
        // Null considered same as FALSE, can be removed
      } else if (Predicate.IS_FALSE_LITERAL.apply(whenExpr)) {
        // This WHEN is always FALSE, so it can be removed.
      } else if (Predicate.IS_TRUE_LITERAL.apply(whenExpr)) {
        // This WHEN is always TRUE, so the cases after it can never be reached.
        elseExpr = thenValue;
        break;
      } else {
        newWhenClauses.add(new CaseWhenClause(whenExpr, thenValue));
      }
    }

    return buildCase(expr, null, newWhenClauses, elseExpr);
  }

  /**
   * Build a rewritten case statement.
   *
   * * If the ELSE clause is a NULL literal, then drops that clause.
   * * If no ELSE, drops any tail WHEN clauses that have NULL as
   *   their value.
   * * If no WHEN clauses remain, returns the ELSE clause (or NULL).
   * * Else the general case has an expression, list of WHEN clauses and
   *   and ELSE clause.
   */
  private Expr buildCase(CaseExpr original, Expr caseExpr,
     List<CaseWhenClause> newWhenClauses, Expr elseExpr) {
    if (elseExpr != null && Expr.IS_NULL_VALUE.apply(elseExpr)) {
      elseExpr = null;
    }
    // Trim final terms with NULL as the THEN value
    if (elseExpr == null) {
      for (int i = newWhenClauses.size() - 1; 0 <= i; i--) {
        if (! Expr.IS_NULL_VALUE.apply(newWhenClauses.get(i).getThenExpr())) {
          break;
        }
        newWhenClauses.remove(i);
      }
    }
    if (! newWhenClauses.isEmpty()) {
      return new CaseExpr(caseExpr, newWhenClauses, elseExpr);
    } else if (elseExpr != null) {
      return elseExpr;
    } else {
      return NullLiteral.create(original.getType());
    }
  }
}
