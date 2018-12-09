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

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;

/**
 * Expression analysis engine:
 *
 * - Depth-first resolution of an expression tree from the children up.
 * - Resolves column and alias names.
 * - Performs type propagation up from children.
 * - Rewrites expressions according to the rules provided.
 * - Computes execution cost.
 *
 * Resolution may replace the original node with a new node.
 *
 * Type propagation is a four-step process:
 * - Compute the "native" type of each node.
 * - Match input argument types to parameter types of functions and
 *   operators.
 * - Insert an implicit cast as needed to convert types.
 * - Use expression rewrite rules to "push" the cast into constants.
 *
 * The rewrite rules may replace one node with a new subtree. Any new nodes
 * require the above process to be repeated on the children. One constraint,
 * however, is that type propagation must be done only once to avoid
 * oscillation. Oscillation occurs in the operators:
 *
 * TINYINT + TINYINT --> SMALLINT
 *
 * Insert casts:
 *
 * CAST(TINYINT AS SMALLINT) + CAST(TINYINT AS SMALLINT) --> SMALLINT
 *
 * If repeated the type propagation, we'd determine the output size should be an
 * INT, and would widen the arguments again.
 */
public class ExprAnalyzer {

  /**
   * Resolve a column reference which may be either an alias or a
   * slot reference. Returns the resolved reference.
   */
  public interface ColumnResolver {
    Expr resolve(SlotRef slotRef) throws AnalysisException;
  }

  /**
   * Resolve a column reference against the set of base tables maintained
   * by the analyzer.
   */
  public static class SlotResolver implements ColumnResolver {
    private final Analyzer analyzer_;

    public SlotResolver(Analyzer analyzer) {
      analyzer_ = analyzer;
    }

    @Override
    public Expr resolve(SlotRef slotRef) throws AnalysisException {
      // TODO: derived slot refs (e.g., star-expanded) will not have rawPath set.
      // Change construction to properly handle such cases.
      Preconditions.checkState(slotRef.getRawPath() != null);
      Path resolvedPath = null;
      try {
        resolvedPath = analyzer_.resolvePath(slotRef.getRawPath(), PathType.SLOT_REF);
      } catch (TableLoadingException e) {
        // Should never happen because we only check registered table aliases.
        throw new IllegalStateException(e);
      }
      Preconditions.checkNotNull(resolvedPath);
      slotRef.resolvedTo(resolvedPath, analyzer_.registerSlotRef(resolvedPath));
      return slotRef;
    }
  }

  private final Analyzer analyzer_;
  private final ColumnResolver colResolver_;
  private final boolean enableRewrites_;
  private int rewriteCount_;

  public ExprAnalyzer(Analyzer analyzer) {
    Preconditions.checkNotNull(analyzer);
    analyzer_ = analyzer;
    colResolver_ = new SlotResolver(analyzer_);
    enableRewrites_ = analyzer_.getQueryCtx().getClient_request().getQuery_options().enable_expr_rewrites;
  }

  /**
   * Perform semantic analysis of a node and all of its children.
   *
   * throws AnalysisException if any errors found.
   * @see ParseNode#analyze(Analyzer)
   */
  public Expr analyze(Expr expr) throws AnalysisException {
    if (expr.isAnalyzed()) return expr;

    // Check the expr child limit.
    if (expr.getChildCount() > Expr.EXPR_CHILDREN_LIMIT) {
      String sql = expr.toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of child " +
          "expressions: %d\nExpression has %s children:\n%s...",
          Expr.EXPR_CHILDREN_LIMIT, expr.getChildCount(), sqlSubstr));
    }

    analyzer_.incrementCallDepth();
    // Check the expr depth limit. Do not print the toSql() to not overflow the stack.
    if (analyzer_.getCallDepth() > Expr.EXPR_DEPTH_LIMIT) {
      throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
          "expression tree: %d", Expr.EXPR_DEPTH_LIMIT));
    }
    for (int i = 0; i < expr.getChildCount(); i++) {
      expr.setChild(i, analyze(expr.getChild(i)));
    }
    analyzer_.decrementCallDepth();
    if (false) {
      // Why is this before resolve?
      expr.computeNumDistinctValues();

      // For now, call existing methods. Create parallel paths, then switch to
      // use those methods.
      expr.analyzeImpl(analyzer_);
      expr.evalCost_ = expr.computeEvalCost();
    } else {
      expr = expr.resolve(colResolver_);
      for (;;) {
        expr.analyzeNode(analyzer_);
        Expr result = expr.rewrite(this);
        if (result == expr) break;
        expr = result;
        rewriteCount_++;
        for (Expr child: expr.getChildren()) {
          analyze(child);
        }
      }
      expr.computeCost();
    }
    expr.analysisDone();
    return expr;
  }

  public ColumnResolver columnResolver() { return colResolver_; }
}
