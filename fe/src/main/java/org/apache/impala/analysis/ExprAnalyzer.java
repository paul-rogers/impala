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

package org.apache.impala.analysis;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.kudu.shaded.com.google.common.base.Preconditions;

/**
 * Drives expression analysis performing rewrites, substitutions
 * and so on. Once analyzed, the expression is effectively immutable.
 * (This is a forward-looking statement, current version is not there
 * yet.)
 */
public class ExprAnalyzer {

  private final Analyzer analyzer_;
  private final ExprRewriter rewriter_;
  private int depth_;
  private int rewriteCount_;
  // No rewrites by default until all clauses
  // are converted.
  private boolean enableRewrites_;

  public ExprAnalyzer(Analyzer analyzer) {
    analyzer_ = analyzer;
    rewriter_ = analyzer_.getExprRewriter();
  }

  /**
   * Analyze a top-level expression.
   *
   * @param expr the un-analyzed expression
   * @return the analyzed expression, which may be different than the
   * unanalyzed version
   * @throws AnalysisException for all analysis errors
   */
  public Expr analyze(Expr expr) throws AnalysisException {
    return analyze(expr, enableRewrites_);
  }

  public Expr analyze(Expr expr, boolean withRewrite) throws AnalysisException {
    // Temporary hack to allow enabling rewrites per clause until
    // all clauses are converted.
    boolean oldRewriteFlag = enableRewrites_;
    enableRewrites_ = withRewrite;
    expr = analyzeExpr(expr);
    enableRewrites_ = oldRewriteFlag;
    return expr;
  }

  /**
   * Perform semantic analysis of node and all of its children.
   * Called for each node in an expression tree.
   *
   * @throws AnalysisException if any errors found.\
   * @see ParseNode#analyze(Analyzer)
   */
  public Expr analyzeExpr(Expr expr) throws AnalysisException {
    if (expr.isAnalyzed()) return expr;

    // Check the expr depth limit. Do not print the toSql() to not overflow the stack.
    if (++depth_ > Expr.EXPR_DEPTH_LIMIT) {
      throw new AnalysisException(String.format(
          "Exceeded the maximum depth of an expression tree (%d).",
          Expr.EXPR_DEPTH_LIMIT));
    }

    // Check the expr child limit.
    if (expr.getChildCount() > Expr.EXPR_CHILDREN_LIMIT) {
      String sql = expr.toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of child " +
          "expressions (%d).\nExpression has %d children:\n%s...",
          Expr.EXPR_CHILDREN_LIMIT, expr.getChildCount(), sqlSubstr));
    }

    for (;;) {
      // Depth-first, bottom up analysis
      for (Expr child: expr.getChildExprs()) {
        analyzeExpr(child);
      }
      // Why is this done before resolving slot refs?
      expr.computeNumDistinctValues();

      // Do all the analysis for the expr subclass before marking the Expr analyzed.
      expr.analyzeImpl(analyzer_);
      expr.evalCost_ = expr.computeEvalCost();
      expr.analysisDone();

      if (!enableRewrites_) break;

      // Rewrite just this level of expression
      Expr result = rewriter_.rewriteNode(expr, analyzer_);
      if (result == expr) break;
      rewriteCount_++;
      expr = result;
      // Less than ideal. Revisit. Only type and cost
      // need be recomputed.
      expr.reset();
      // Literals don't become un-analyzed.
      if (expr.isAnalyzed()) break;

      // Loop to re-analyze and possibly rewrite children
      // which may have been newly created.
    }
    depth_--;
    return expr;
  }

  /**
   * Temporary backward-compatibility feature to disable
   * integrated rewrites so that the existing rewriter tests
   * pass.
   * TODO: To be removed as work proceeds.
   */
  public void enableRewrites() {
    enableRewrites_ = true;
  }

  public int rewriteCount() { return rewriteCount_; }

}
