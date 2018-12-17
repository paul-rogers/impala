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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.common.AnalysisException;

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
      simplified = ((FunctionCallExpr) expr).simplifyConditionals();
      simplified = simplified == null ? expr : simplified;
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
      simplified = analyzer.analyzeAndRewrite(simplified);
      if (expr.contains(Expr.isAggregatePredicate())
          && !simplified.contains(Expr.isAggregatePredicate())) {
        return expr;
      }
    }
    return simplified;
  }

  /**
   * Simplifies compound predicates with at least one BoolLiteral child, which
   * NormalizeExprsRule ensures will be the left child,  according to the following rules:
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
    Expr result = expr.simplify();
    return result == null ? expr : result;
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
    return expr.rewrite(analyzer.exprAnalyzer());
  }
}
