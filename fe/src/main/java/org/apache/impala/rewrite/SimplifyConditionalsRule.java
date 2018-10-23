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
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
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
   * Simplifies IF by returning the corresponding child if the condition has a constant
   * TRUE, FALSE, or NULL (equivalent to FALSE) value.
   */
  private Expr simplifyIfFn(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 3);
    if (expr.getChild(0) instanceof BoolLiteral) {
      if (((BoolLiteral) expr.getChild(0)).getValue()) {
        // IF(TRUE)
        return expr.getChild(1);
      } else {
        // IF(FALSE)
        return expr.getChild(2);
      }
    } else if (expr.getChild(0) instanceof NullLiteral) {
      // IF(NULL)
      return expr.getChild(2);
    }
    return expr;
  }

  /**
   * Simplifies IFNULL if the condition is a literal, using the
   * following transformations:
   *   IFNULL(NULL, x) -> x
   *   IFNULL(a, x) -> a, if a is a non-null literal
   *   IFNULL(a, x) -> CASE WHEN a IS NULL THEN x ELSE a END
   */
  private Expr simplifyIfNullFn(FunctionCallExpr expr) {
    Preconditions.checkState(expr.getChildren().size() == 2);
    Expr child0 = expr.getChild(0);
    if (child0 instanceof NullLiteral) return expr.getChild(1);
    if (child0.isLiteral()) return child0;

    // Transform into a CASE expression
    return new CaseExpr(null,
        Lists.newArrayList(
            new CaseWhenClause(
                new IsNullPredicate(expr.getChild(0), false),
                expr.getChild(1))),
        expr.getChild(0));
  }

  /**
   * Simplify COALESCE by skipping leading nulls and applying the following transformations:
   * COALESCE(null, a, b) -> COALESCE(a, b);
   * COALESCE(<literal>, a, b) -> <literal>, when literal is not NullLiteral;
   * COALESCE(a, <literal>, b) -> COALESCE(a, <literal) when literal is not NullLiteral;
   * COALESCE(a, b) -->
   *     CASE WHEN a IS NOT NULL THEN a
   *          WHEN b IS NOT NULL THEN b END
   */
  private Expr simplifyCoalesceFn(FunctionCallExpr expr) {
    List<Expr> revised = new ArrayList<>();
    for (Expr childExpr : expr.getChildren()) {
      // Skip nulls.
      if (childExpr.isNullLiteral()) continue;
      revised.add(childExpr);
      if (childExpr.isLiteral()) break;
    }
    if (revised.isEmpty()) { return NullLiteral.create(expr.getType()); }
    if (revised.size() == 1) { return revised.get(0); }

    List<CaseWhenClause> whenList = new ArrayList<>();
    for (int i = 0; i < revised.size() - 1; i++) {
      Expr childExpr = revised.get(i);
      whenList.add(new CaseWhenClause(
          // TODO: Clone child expr here?
          new IsNullPredicate(childExpr, true), childExpr));
    }
    return new CaseExpr(null, whenList,
        revised.get(revised.size() - 1));


//    int numChildren = expr.getChildren().size();
//    Expr result = NullLiteral.create(expr.getType());
//    for (int i = 0; i < numChildren; ++i) {
//      Expr childExpr = expr.getChildren().get(i);
//      // Skip leading nulls.
//      if (childExpr.isNullLiteral()) continue;
//      if ((i == numChildren - 1) || childExpr.isLiteral()) {
//        result = childExpr;
//      } else if (i == 0) {
//        result = expr;
//      } else {
//        List<Expr> newChildren = Lists.newArrayList(expr.getChildren().subList(i, numChildren));
//        result = new FunctionCallExpr(expr.getFnName(), newChildren);
//      }
//      break;
//    }
//    return result;
  }

  private Expr simplifyFunctionCallExpr(FunctionCallExpr expr) {
    switch (expr.getFnName().getFunction()) {
    case "if":
      return simplifyIfFn(expr);
    case "coalesce":
      return simplifyCoalesceFn(expr);
    case "isnull":
    case "nvl":
    case "ifnull":
      return simplifyIfNullFn(expr);
    case "decode":
      return rewriteDecodeFn(expr);
    case "nvl2":
      return rewriteNvl2Fn(expr);
    case "nullif":
      return rewriteNullIfFn(expr);
    default:
      return expr;
    }
  }

  /**
   * Simplifies compound predicates with at least one BoolLiteral child, which
   * NormalizeExprsRule ensures will be the left child,  according to the following rules:
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
      if (((BoolLiteral) leftChild).getValue()) {
        // TRUE AND 'expr', so return 'expr'.
        return expr.getChild(1);
      } else {
        // FALSE AND 'expr', so return FALSE.
        return leftChild;
      }
    } else if (expr.getOp() == CompoundPredicate.Operator.OR) {
      if (((BoolLiteral) leftChild).getValue()) {
        // TRUE OR 'expr', so return TRUE.
        return leftChild;
      } else {
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
   * Note that FunctionalCallExpr.createExpr() converts "nvl2" into "if",
   * "decode" into "case", and "nullif" into "if".
   */
  private Expr simplifyCaseExpr(CaseExpr expr, Analyzer analyzer)
      throws AnalysisException {
    Expr caseExpr = expr.hasCaseExpr() ? expr.getChild(0) : null;
    if (expr.hasCaseExpr() && !caseExpr.isLiteral()) return expr;

    int numChildren = expr.getChildren().size();
    int loopStart = expr.hasCaseExpr() ? 1 : 0;
    // Check and return early if there's nothing that can be simplified.
    boolean canSimplify = false;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      if (expr.getChild(i).isLiteral()) {
        canSimplify = true;
        break;
      }
    }
    if (!canSimplify) return expr;

    // Contains all 'when' clauses with non-constant conditions, used to construct the new
    // CASE expr while removing any FALSE or NULL cases.
    List<CaseWhenClause> newWhenClauses = new ArrayList<CaseWhenClause>();
    // Set to THEN of first constant TRUE clause, if any.
    Expr elseExpr = null;
    for (int i = loopStart; i < numChildren - 1; i += 2) {
      Expr child = expr.getChild(i);
      if (child instanceof NullLiteral) continue;

      Expr whenExpr;
      if (expr.hasCaseExpr()) {
        if (child.isLiteral()) {
          BinaryPredicate pred = new BinaryPredicate(
              BinaryPredicate.Operator.EQ, caseExpr, expr.getChild(i));
          pred.analyze(analyzer);
          whenExpr = analyzer.getConstantFolder().rewrite(pred, analyzer);
        } else {
          whenExpr = null;
        }
      } else {
        whenExpr = child;
      }

      if (whenExpr instanceof BoolLiteral) {
        if (((BoolLiteral) whenExpr).getValue()) {
          if (newWhenClauses.size() == 0) {
            // This WHEN is always TRUE, and any cases preceding it are constant
            // FALSE/NULL, so just return its THEN.
            return expr.getChild(i + 1).castTo(expr.getType());
          } else {
            // This WHEN is always TRUE, so the cases after it can never be reached.
            elseExpr = expr.getChild(i + 1);
            break;
          }
        } else {
          // This WHEN is always FALSE, so it can be removed.
        }
      } else {
        newWhenClauses.add(new CaseWhenClause(child, expr.getChild(i + 1)));
      }
    }

    if (expr.hasElseExpr() && elseExpr == null) elseExpr = expr.getChild(numChildren - 1);
    if (newWhenClauses.size() == 0) {
      // All of the WHEN clauses were FALSE, return the ELSE.
      if (elseExpr == null) return NullLiteral.create(expr.getType());
      return elseExpr;
    }
    return new CaseExpr(caseExpr, newWhenClauses, elseExpr);
  }

  /**
   * Rewrite DECODE constructs an equivalent CaseExpr representation.
   * The backend implementation is
   * always the "case" function. CASE always returns the THEN corresponding to the leftmost
   * WHEN that is TRUE, or the ELSE (or NULL if no ELSE is provided) if no WHEN is TRUE.
   * <p>
   * The internal representation of<pre><code>
   *   DECODE(expr, key_expr, val_expr [, key_expr, val_expr ...] [, default_val_expr])
   * </code></pre>
   * has a pair of children for each pair of key/val_expr and an additional child if the
   * default_val_expr was given. The first child represents the comparison of expr to
   * key_expr. Decode has three forms:
   * <dl>
   * <dt><code>DECODE(expr, null_literal, val_expr)<code></dt>
   * <dd><code>child[0] = IsNull(expr)</code></dd>
   * <dt><code>DECODE(expr, non_null_literal, val_expr)<code></dt>
   * <dd><code>child[0] = Eq(expr, literal)
   * <dt><code>DECODE(expr1, expr2, val_expr)<code></dt>
   * <dd><code>child[0] = Or(And(IsNull(expr1), IsNull(expr2)),  Eq(expr1, expr2))</code></dd>
   * </dl>
   *
   * The children representing val_expr (child[1]) and default_val_expr (child[2]) are
   * simply the exprs themselves.
   * <p>
   * Example of equivalent CASE for DECODE(foo, 'bar', 1, col, 2, NULL, 3, 4):
   * <pre><code>
   *   CASE
   *     WHEN foo = 'bar' THEN 1   -- no need for IS NULL check
   *     WHEN foo IS NULL AND col IS NULL OR foo = col THEN 2
   *     WHEN foo IS NULL THEN 3  -- no need for equality check
   *     ELSE 4
   *   END
   * </code><pre>
   * <p>
   * The DECODE behavior is basically the same as the hasCaseExpr_ version of CASE.
   * Though there is one difference. NULLs are considered equal when comparing the
   * argument to be decoded with the candidates. This differences is for compatibility
   * with Oracle. http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions040.htm.
   * To account for the difference, the CASE representation will use the non-hasCaseExpr_
   * version.
   * <p>
   * The return type of DECODE differs from that of Oracle when the third argument is
   * the NULL literal. In Oracle the return type is STRING. In Impala the return type is
   * determined by the implicit casting rules (i.e. it's not necessarily a STRING). This
   * is done so seemingly normal usages such as DECODE(int_col, tinyint_col, NULL,
   * bigint_col) will avoid type check errors (STRING incompatible with BIGINT).
   */
  private Expr rewriteDecodeFn(FunctionCallExpr decodeExpr) {

    List<CaseWhenClause> whenTerms = new ArrayList<>();

    int childIdx = 0;
    Expr encoded = decodeExpr.getChild(childIdx++);

    // Add the key_expr/val_expr pairs
    while (childIdx + 2 <= decodeExpr.getChildren().size()) {
      Expr candidate = decodeExpr.getChild(childIdx++);
      Expr whenExpr;
      if (candidate.isNullLiteral()) {
        // An example case is DECODE(foo, NULL, bar), since NULLs are considered
        // equal, this becomes CASE WHEN foo IS NULL THEN bar END.
        whenExpr = new IsNullPredicate(encoded.clone(), false);
      } else if (candidate.isLiteral() || encoded.isLiteral()) {
        // One or the other terms is a non-null literal, so
        // decode(foo, 10, bar) - CASE WHEN foo = 10 THEN bar END
        // decode(10, foo. bar) - CASE WHEN 10 = foo THEN bar END
        whenExpr = new BinaryPredicate(
            BinaryPredicate.Operator.EQ, encoded.clone(), candidate);
      } else {
        // Non-literal. Nulls match, so use IS NOT DISTINCT
        whenExpr =
            new BinaryPredicate(BinaryPredicate.Operator.NOT_DISTINCT,
                encoded.clone(), candidate);
//        children_.add(new CompoundPredicate(CompoundPredicate.Operator.OR,
//            new CompoundPredicate(CompoundPredicate.Operator.AND,
//                encodedIsNull.clone(), new IsNullPredicate(candidate, false)),
//            new BinaryPredicate(BinaryPredicate.Operator.EQ, encoded.clone(),
//                candidate)));
      }

      // Add the value
      whenTerms.add(
          new CaseWhenClause(whenExpr, decodeExpr.getChild(childIdx++)));
    }

    // Add the default value
    Expr elseExpr = null;
    if (childIdx < decodeExpr.getChildren().size()) {
      elseExpr = decodeExpr.getChild(childIdx);
    }

    CaseExpr caseExpr = new CaseExpr(null, whenTerms, elseExpr);

    // Check that these exprs were cloned above, as reusing the same Expr object in
    // different places can lead to bugs, eg. if the Expr has multiple parents, they may
    // try to cast it to different types.
    Preconditions.checkState(!caseExpr.contains(encoded));
    return caseExpr;
  }

  private Expr rewriteNvl2Fn(FunctionCallExpr expr) {
    List<Expr> plist = expr.getParams().exprs();
    Expr head = plist.get(0);
    Expr ifNotNull = plist.get(1);
    Expr ifNull = plist.get(2);
    if (head.isNullLiteral()) { return ifNull; }
    if (head.isLiteral()) { return ifNotNull; }
    return new CaseExpr(null,
        Lists.newArrayList(
          new CaseWhenClause(
            new IsNullPredicate(head, true),
            ifNotNull)),
        ifNull); // NULL
  }

  /**
   * Rewrite of nullif(x, y)
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
    return new CaseExpr(null,
        Lists.newArrayList(
          new CaseWhenClause(
            new BinaryPredicate(BinaryPredicate.Operator.DISTINCT_FROM, head,
                tail), // x IS DISTINCT FROM y
            head.clone())), // x
        null); // NULL
  }

}
