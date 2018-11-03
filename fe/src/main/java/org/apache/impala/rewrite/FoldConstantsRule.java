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
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;

import org.apache.impala.common.AnalysisException;

/**
 * This rule replaces a constant Expr with its equivalent LiteralExpr by evaluating the
 * Expr in the BE. Exprs that are already LiteralExprs are not changed.
 *
 * TODO: Expressions fed into this rule are currently not required to be analyzed
 * in order to support constant folding in expressions that contain unresolved
 * references to select-list aliases (such expressions cannot be analyzed).
 * The cross-dependencies between rule transformations and analysis are vague at the
 * moment and make rule application overly complicated.
 *
 * Examples:
 * 1 + 1 + 1 --> 3
 * toupper('abc') --> 'ABC'
 * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
 */
public class FoldConstantsRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new FoldConstantsRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    // Avoid calling Expr.isConstant() because that would lead to repeated traversals
    // of the Expr tree. Assumes the bottom-up application of this rule. Constant
    // children should have been folded at this point.
    //
    // However, this rule may produce an expression the form CAST(NULL as type),
    // which are not literals. So, we call IS_LITERAL_VALUE that checks
    // for an actual literal, or the cast of a null (which is literal enough
    // for our needs.)
    //
    // We may find an expression of the form (CAST 1.8 AS DECIMAL(...)).
    // Don't treat this as a child literal: it is the result of a prior
    // rewrite attempt that produced an overflow or other error, so we leave
    // it unchanged.
    for (Expr child: expr.getChildren()) {
      if (!Expr.IS_LITERAL_VALUE.apply(child)) return expr;
    }

    // Do not constant fold cast(null as dataType) because we cannot preserve the
    // cast-to-types and that can lead to query failures, e.g., CTAS
    if (Expr.IS_NULL_VALUE.apply(expr)) { return expr; }

    // If the is a literal, it can't get any simpler. If it is not
    // a constant, we can't evaluate it.
    if (Expr.IS_LITERAL.apply(expr) || !expr.isConstant()) return expr;

    // Do not constant fold cast(null as dataType) because we cannot preserve the
    // cast-to-types and that can lead to query failures, e.g., CTAS
    if (Expr.IS_NULL_LITERAL.apply(expr)) { return expr; }

    // Analyze constant exprs, if necessary. Note that the 'expr' may become
    // non-constant after analysis (e.g., aggregate functions).
    if (!expr.isAnalyzed()) {
      expr.analyze(analyzer);
      if (!expr.isConstant()) return expr;
    }

    Expr result = LiteralExpr.create(expr, analyzer.getQueryCtx());

    // Preserve original type so parent Exprs do not need to be re-analyzed.
    // This may mean that an expression of the for CAST(1.8 AS DECIMAL(38,28))
    // is preserved. In this case, though this expression looks like a literal,
    // it is not because it may evaluate to NULL at runtime due to overflow;
    // the above must be treated as non-constant.
    if (result == null) {
      // The expression is not really a constant. Avoid repeated
      // passes through this rule.
      expr.becomeNonConstant();
      return expr;
    } else {
      // Change the type associated with the literal to the
      // desired type. Does not produce CAST(x AS type).
      return result.castTo(expr.getType());
    }
  }

  private FoldConstantsRule() {}
}
