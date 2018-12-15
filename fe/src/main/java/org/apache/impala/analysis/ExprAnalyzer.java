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
    NodeAnalyzer nodeAnalyzer = new NodeAnalyzer(this, expr);
    nodeAnalyzer.analyze();
    return nodeAnalyzer.expr_;
  }

  private static class NodeAnalyzer {
    ExprAnalyzer exprAnalyzer_;
    Expr expr_;
    boolean preserveType_;

    public NodeAnalyzer(ExprAnalyzer exprAnalyzer, Expr node) {
      exprAnalyzer_ = exprAnalyzer;
      expr_ = node;
    }

    public NodeAnalyzer(NodeAnalyzer parent, Expr node) {
      exprAnalyzer_ = parent.exprAnalyzer_;
      preserveType_ = parent.preserveType_;
      expr_ = node;
    }

    public void analyze() throws AnalysisException {
      // Always do at least the constant folding check.
      if (expr_.isAnalyzed()) {
        foldConstants();
        return;
      }
      check();

      // Resolve is for leaves; OK not to have checked children
      expr_ = expr_.resolve(exprAnalyzer_.colResolver_);
      // Analyze
      analyzeNode(expr_);
      // Rewrite
      rewrite();
      // Finalize
      exprAnalyzer_.analyzer_.decrementCallDepth();
      // Rewrite may have returned a child already analyzed
      if (expr_.isAnalyzed()) return;
      expr_.computeCost();
      expr_.analysisDone();
      // Fold constants last; requires the node be marked as analyzed.
      foldConstants();
    }

    private void check() throws AnalysisException {
      // Check the expr depth limit. Do not print the toSql() avoid stack overflow.
      Analyzer analyzer = exprAnalyzer_.analyzer_;
      analyzer.incrementCallDepth();
      if (analyzer.getCallDepth() > Expr.EXPR_DEPTH_LIMIT) {
        throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
            "expression tree: %d", Expr.EXPR_DEPTH_LIMIT));
      }
      // Check the expr child limit.
      if (expr_.getChildCount() > Expr.EXPR_CHILDREN_LIMIT) {
        String sql = expr_.toSql();
        String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
        throw new AnalysisException(String.format("Exceeded the maximum number of child " +
            "expressions: %d\nExpression has %s children:\n%s...",
            Expr.EXPR_CHILDREN_LIMIT, expr_.getChildCount(), sqlSubstr));
      }
    }

    private void analyzeNode(Expr node) throws AnalysisException {
      int pass = 1;
      for (;;) {
        for (int i = 0; i < node.getChildCount(); i++) {
          NodeAnalyzer childAnalyzer = new NodeAnalyzer(this, node.getChild(i));
          childAnalyzer.analyze();
          node.setChild(i, childAnalyzer.expr_);
        }
        if (pass == 2) break;
        node.analyzeNode(exprAnalyzer_.analyzer_);

        // Analysis can revise children, which may constant-fold those children. First,
        // determine if this expression must preserve type because either the
        // parent or any child wants to preserve type. Then, analyze the child with the
        // resulting type preservation decision.
        //
        // 2 + 3 --> 5:TINYINT
        // tinyint_col + 4 --> SMALLINT
        //
        // For pure-literal expressions, use the natural type of the result.
        // Pure-literal expression are those without explicit casts. Due to prior
        // folding events, we only need to check one level of children.
        if (!preserveType_) {
          for (int i = 0; i < node.getChildCount(); i++) {
            preserveType_ = node.getChild(i).hasExplicitType();
            if (preserveType_) break;
          }
        }
        pass = 2;
      }
    }

    private void rewrite() throws AnalysisException {
      if (exprAnalyzer_.rewriteMode_ == RewriteMode.NONE) return;
      for (;;) {
        Expr result = expr_.rewrite(exprAnalyzer_.rewriteMode_);
        if (result == expr_) break;
        analyzeNode(result);

        // IMPALA-5125: We can't eliminate aggregates as this may change the meaning of the
        // query, for example:
        // 'select if (true, 0, sum(id)) from alltypes' != 'select 0 from alltypes'
        if (expr_.contains(Expr.isAggregatePredicate()) &&
            !result.contains(Expr.isAggregatePredicate())) {
          break;
        }
        expr_ = result;
        exprAnalyzer_.rewriteCount_++;
      }
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
    private void foldConstants() throws AnalysisException {
      // Constant folding is an optional rewrite
      if (exprAnalyzer_.rewriteMode_ != RewriteMode.OPTIONAL) return;
      // Only required rewrites are enabled and this is a pure literal fold
      //(rewriteMode_ == RewriteMode.REQUIRED && !preserveType) ||
      // But can't fold non-constant expressions (relies on the constant
      // flag being cached in Expr.analysisDone())
      if (!expr_.isConstant()) return;
      // If this is already a literal, there is nothing to fold
      if (Expr.IS_LITERAL.apply(expr_)) return;

      if (expr_ instanceof CastExpr) {
        CastExpr castExpr = (CastExpr) expr_;

        // Do not constant fold cast(null as dataType) because we cannot preserve the
        // cast-to-types and that can lead to query failures, e.g., CTAS
        // Testing change:
        // Convert CAST(NULL AS <type>) to a null literal of the given type
        if (Expr.IS_NULL_LITERAL.apply(castExpr.getChild(0))) {
          // Trying using a typed null literal
          expr_ = new NullLiteral(castExpr.getType());
          return;
        }
        preserveType_ |= castExpr.hasExplicitType();
      }

      Expr result;
      if (preserveType_) {
        // Must preserve the expression's type. Examples:
        // CAST(1 AS INT) --> must be of type INT (natural type is TINYINT)
        // CAST(1 AS INT) + CAST(2 AS SMALLINT) --> 3:BIGINT (normal INT
        //     addition result type
        // CAST(1 AS TINYINT) + CAST(1 AS TINYINT) --> 2:SMALLINT
        // CAST(1 AS TINYINT) + 1 --> 2:SMALLINT
        result = LiteralExpr.typedEval(expr_, exprAnalyzer_.analyzer_.getQueryCtx());
      } else {
        // No cast, eval to the natural type
        // 1 + 1 --> 2:TINYINT
        result = LiteralExpr.untypedEval(expr_, exprAnalyzer_.analyzer_.getQueryCtx());
      }

      if (result != null) expr_ = result;
    }
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
