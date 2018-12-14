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

  public enum RewriteMode {
    NONE,
    REQUIRED,
    OPTIONAL
  }

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
  private RewriteMode rewriteMode_ = RewriteMode.REQUIRED;
  private int rewriteCount_;

  public ExprAnalyzer(Analyzer analyzer) {
    Preconditions.checkNotNull(analyzer);
    analyzer_ = analyzer;
    colResolver_ = new SlotResolver(analyzer_);
    rewriteMode_ =
        analyzer_.getQueryCtx().getClient_request().getQuery_options().enable_expr_rewrites
        ? RewriteMode.OPTIONAL : RewriteMode.REQUIRED;
  }

  /**
   * Perform semantic analysis of a node and all of its children. A node that
   * is marked analyzed may still need to be rewritten (in particular, an
   * explicit cast of a literal.)
   *
   * throws AnalysisException if any errors found.
   * @see ParseNode#analyze(Analyzer)
   */
  public Expr analyze(Expr expr) throws AnalysisException {
    return analyze(expr, false);
  }

  public Expr analyze(Expr expr, boolean preserveType) throws AnalysisException {
    // Always to at least the constant folding check.
    if (expr.isAnalyzed()) {
      return foldConstants(expr, preserveType);
    }
    // Check the expr depth limit. Do not print the toSql() avoid stack overflow.
    analyzer_.incrementCallDepth();
    if (analyzer_.getCallDepth() > Expr.EXPR_DEPTH_LIMIT) {
      throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
          "expression tree: %d", Expr.EXPR_DEPTH_LIMIT));
    }
    // Check the expr child limit.
    if (expr.getChildCount() > Expr.EXPR_CHILDREN_LIMIT) {
      String sql = expr.toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of child " +
          "expressions: %d\nExpression has %s children:\n%s...",
          Expr.EXPR_CHILDREN_LIMIT, expr.getChildCount(), sqlSubstr));
    }

    // Resolve is for leaves; OK not to have checked children
    expr = expr.resolve(colResolver_);
    for (;;) {
      // Analyze
      for (int i = 0; i < expr.getChildCount(); i++) {
        expr.setChild(i, analyze(expr.getChild(i), preserveType));
      }
      expr.analyzeNode(analyzer_);

      // Analysis can revise children, which may constant-fold those children. First,
      // determine if this expression must preserve type which is true if either the
      // parent or any child wants to preserve type. Then, analyze the child with the
      // resulting type preservation decision.
      //
      // 2 + 3 --> 5:TINYINT
      // tinyint_col + 4 --> SMALLINT
      //
      // For pure-literal expressions, use the natural type of the result.
      // Pure-literal expression are those without explicit casts. Due to prior
      // folding events, we only need to check one level of children.
      if (!preserveType) {
        for (int i = 0; i < expr.getChildCount(); i++) {
          preserveType = expr.getChild(i).hasExplicitType();
          if (preserveType) break;
        }
      }
      for (int i = 0; i < expr.getChildCount(); i++) {
        expr.setChild(i, analyze(expr.getChild(i), preserveType));
      }

      // Rewrite
      if (rewriteMode_ == RewriteMode.NONE) break;
      Expr result = expr.rewrite(rewriteMode_);
      if (result == expr) break;
      expr = result;
      rewriteCount_++;
    }
    analyzer_.decrementCallDepth();
    expr.computeCost();
    expr.analysisDone();
    // Fold constants last; requires the node be marked as analyzed.
    return foldConstants(expr, preserveType);
  }

  /**
   * Replace a constant Expr with its equivalent LiteralExpr by evaluating the
   * Expr in the BE. Exprs that are already LiteralExprs are not changed.
   *
   * Examples:
   * 1 + 1 + 1 --> 3
   * toupper('abc') --> 'ABC'
   * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
   */
  private Expr foldConstants(Expr expr, boolean preserveType) throws AnalysisException {
    // Constant folding is an optional rewrite
    if (rewriteMode_ != RewriteMode.OPTIONAL ||
        // Only required rewrites are enabled and this is a pure literal fold
        //(rewriteMode_ == RewriteMode.REQUIRED && !preserveType) ||
        // But can't fold non-constant expressions (relies on the constant
        // flag being cached in Expr.analysisDone())
        !expr.isConstant() ||
        // If this is already a literal, there is nothing to fold
        Expr.IS_LITERAL.apply(expr)) return expr;

    if (expr instanceof CastExpr) {
      CastExpr castExpr = (CastExpr) expr;

      // Do not constant fold cast(null as dataType) because we cannot preserve the
      // cast-to-types and that can lead to query failures, e.g., CTAS
      // Testing change:
      // Convert CAST(NULL AS <type>) to a null literal of the given type
      if (Expr.IS_NULL_LITERAL.apply(castExpr.getChild(0))) {
        // Trying using a typed null literal
        return new NullLiteral(castExpr.getType());
      }
      preserveType |= castExpr.hasExplicitType();
    }

    Expr result;
    if (preserveType) {
      // Must preserve the expression's type. Examples:
      // CAST(1 AS INT) --> must be of type INT (natural type is TINYINT)
      // CAST(1 AS INT) + CAST(2 AS SMALLINT) --> 3:BIGINT (normal INT
      //     addition result type
      // CAST(1 AS TINYINT) + CAST(1 AS TINYINT) --> 2:SMALLINT
      // CAST(1 AS TINYINT) + 1 --> 2:SMALLINT
      result = LiteralExpr.typedEval(expr, analyzer_.getQueryCtx());
    } else {
      // No cast, eval to the natural type
      // 1 + 1 --> 2:TINYINT
      result = LiteralExpr.untypedEval(expr, analyzer_.getQueryCtx());
    }

    return result == null ? expr : result;
  }

  public void analyzeWithoutRewrite(Expr expr) throws AnalysisException {
    RewriteMode oldMode = rewriteMode_;
    rewriteMode_ = RewriteMode.NONE;
    try {
      Expr result = analyze(expr);
      Preconditions.checkState(result == expr);
    } finally {
      rewriteMode_ = oldMode;
    }
  }

  public ColumnResolver columnResolver() { return colResolver_; }
  public boolean perrormRequiredRewrites() { return rewriteMode_ != RewriteMode.NONE; }
  public boolean performOptionalRewrites() { return rewriteMode_ == RewriteMode.OPTIONAL; }
  public int rewriteCount() { return rewriteCount_; }
}
