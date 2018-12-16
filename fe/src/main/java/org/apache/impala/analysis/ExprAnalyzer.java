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
  private int constantFoldCount_;

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
   * Top-level expression typically have no pre-defined type, the type is taken
   * from the expression itself. If the expression is constant, the type is
   * the "natural" (smallest) type of the numeric result. For expressions that
   * include columns, the columns drive type propagation up the tree to determines
   * the result type.
   *
   * throws AnalysisException if any errors found.
   * @see ParseNode#analyze(Analyzer)
   */
  public Expr analyze(Expr expr) throws AnalysisException {
    return analyze(expr, false);
  }

  public enum State { ANALYZE, REWRITE, CHECK_REWRITE, DONE };

  /**
   * Implement per-node analysis. This process is quite complex due to the tasks
   * required: resolution, analysis, rewrite.
   *
   * Analysis is done via recursive calls. Impala supports very deep expression
   * trees: a depth which rival the maximum stack size. Recursive calls must come
   * directly from this method in order to minimize stack depth. (That is, we
   * cannot move processing to a subroutine which calls children as that doubles
   * the stack depth.)
   *
   * Analysis is done in five major steps:
   *
   * - Resolve symbols (column refs)
   * - Analyze children
   * - Analyze the node
   * - Rewrite the node
   * - Compute selectivity and cost
   *
   * Each step, however is complex. In general, each step starts with analyzing
   * children. So, analyzing the node may introduce new nodes (such as casts) which
   * must be analyzed. Similarly rewrites can introduce new children. Rewrites can
   * require multiple passes as the node evolves from the start to final state.
   *
   * Resolution can result in replacing the original node with a different one.
   * For example alias resolution (not yet supported here) replaces a column-ref
   * with the target expression. Resolution can also insert functions, such as for
   * masking secure columns.
   *
   * Child analysis requires propagating two child properties to the current node:
   *
   * - Type preservation (see below)
   * - Whether the node contains an aggregate expression (not yet done)
   *
   * Rewrites are of two kinds:
   *
   * - Required: must be done for the query to run. For example, the AST contains
   *   a BETWEEN node, but the BE does not support this operator. There is a required
   *   rewrite: a BETWEEEN b AND c --> a >= b AND a <= c.
   * - Optional: all other rewrites are optional: the BE can execute the original
   *   AST, but perhaps more slowly than the rewritten form.
   *
   * This function provides three rewrite modes:
   *
   * - OPTIONAL: Perform optional (and required) rewrites. This is the normal
   *   state.
   * - REQUIRED: Perform only required rewrites so that the query can run, but
   *   none of the optional rewrites. This is sometimes enabled because, historically,
   *   rewrites were a source of bugs.
   * - NONE: Perform no rewrites, not even the required ones. This mode is used
   *   only for testing, as it can produce an invalid AST.
   *
   * Rewrites are conditional: they are rejected if they would remove the only
   * aggregate expression within that expression subtree. See IMPALA-5125.
   * This function provides brute-force protection: if the rewritten expression
   * removes the last aggregation, the rewrite is rejected. Required rewrites must
   * obviously be done to avoid rejection. (The BETWEEN rewrite is safe.) If the
   * brute-force approach is too blunt for a particular rewrite, the rewrite
   * implementation can implement its own, specialized solution (in which case
   * the check here will never reject the rewrite.)
   *
   * Most rewrites are node-specific and are done in the class for that node.
   * Constant-folding, however, is generic to all nodes and is done in this function.
   *
   * The above constraints lead to an implementation that makes multiple passes over
   * the expression, each time starting by analyzing children (which will do nothing
   * if the children were previously analyzed.) Then, the next step in the process
   * is performed: ANALYZE --> (REWRITE --> CHECK_REWRITE)* --> DONE
   *
   * The function uses a simple rule to determine if a rewrite occurred: the
   * rewritten node is different than the original node. This means that rewrite
   * implementations MUST return a new node if rewrite occurs; obscure bugs will
   * result if a node is rewritten "in place". Further, rewriting in place breaks
   * the aggregate check described above.
   *
   * Analysis performs constant folding, as the last step. Constant-folding produces
   * a literal, which needs no further analysis. Analyzing a function introduces
   * pre-analyzed implicit casts, which may require constant folding. Therefore,
   * if we analyze an node which is already analyzed, we still attempt to apply
   * constant-folding.
   *
   * Constant folding handles two subtly different cases:
   *
   * - Normal expressions which preserve type. That is,
   *   tinyint_col:TINYINT + tinyint_col:TINYINT --> SMALLINT
   *   which casts both columns to SMALLINT. If we have instead
   *   tinyint_col:TINYINT + 1:TINYINT --> SMALLINT,
   *   the type must also be preserved. Similarly, if we have
   *   1:TINYINT + 2:TINYINT + tinyint_col:TINYINT --> SMALLINT
   *   When we constant-fold 1 + 2, we must preserve the type of SMALLINT
   *   specified by the addition with tinyint_col.
   * - Literal-only expressions which should take the type of the final value.
   *   here,
   *   1:TINYINT + 1:TINYINT --> 2:TINYINT
   *   That is, the resulting type is as if the user had simply typed 2 in the
   *   SQL rather than 1 + 1.
   *
   * When expressions are used as partition keys, the literal-only (do not preserve
   * type) version is required, else 1 + 1 + 1 + 1 ends up as a BIGINT and cannot
   * be used for, say, a SMALLINT value. To make this work, the general rule is that
   * a top-level expression has no preferred type. Literal-only expressions also have
   * no preferred type. But, once a column or other typed entity appears, it imposes
   * a type from there up the expression tree.
   *
   * @param expr the expression to analyze and rewrite
   * @param preserveType whether to preserve the type given by the expression node,
   * or to allow changing constant-folded expressions to their "natural" type
   * @return the analyzed, rewritten expression which may be different than the
   * input expression
   * @throws AnalysisException for all errors
   */
  public Expr analyze(Expr expr, boolean preserveType) throws AnalysisException {
    // Always to at least the constant folding check.
    if (expr.isAnalyzed()) {
      return foldConstants(expr, preserveType);
    }
    check(expr);
    State state = State.ANALYZE;
    Expr beforeRewrite = null;
    while (state != State.DONE) {
      // Analyze children
      for (int i = 0; i < expr.getChildCount(); i++) {
        expr.setChild(i, analyze(expr.getChild(i), preserveType));
      }
      switch (state) {
      case ANALYZE:
        expr = expr.resolve(colResolver_);
        preserveType |= checkPreserveType(expr);
        expr.analyzeNode(analyzer_);
        // Analysis can revise children, which may constant-fold those children, or
        // may introduce un-analyzed children. Reanalyze children.
        state = (rewriteMode_ == RewriteMode.NONE) ? State.DONE : State.REWRITE;
        break;
      case REWRITE:
        beforeRewrite = expr;
        expr = beforeRewrite.rewrite(rewriteMode_);
        // Done if no rewrite. This works ONLY if rewrites replace nodes, not
        // rewrite them in place. As a result, all rewrites MUST create new nodes.
        // Otherwise, validate the rewrite after analyzing potentially
        // new children.
        state = (expr == beforeRewrite) ? state = State.DONE : State.CHECK_REWRITE;
        break;
      case CHECK_REWRITE:
        expr.analyzeNode(analyzer_);
        finish(expr); // Checks below require completed analysis
        if (!rewriteIsValid(beforeRewrite, expr)) {
          // Not valid, reject rewrite and return
          expr = beforeRewrite;
          state = State.DONE;
        } else {
          // Rewrite OK. Repeat for cases of compound rewrite:
          // id = 0 AND TRUE --> TRUE AND id = 0
          // TRUE AND id = 0 --> id = 0
          rewriteCount_++;
          // Rewrite may have returned a child already analyzed and rewritten
          state = expr.isAnalyzed() ? state = State.DONE : State.REWRITE;
        }
        break;
      default:
        throw new IllegalStateException("Invalid state: " + state);
      }
    }
    // Finalize
    analyzer_.decrementCallDepth();
    finish(expr);
    // Fold constants last; requires the node be marked as analyzed.
    // Result of folding is a literal which requires no further analysis
    return foldConstants(expr, preserveType);
  }

  private void check(Expr expr) throws AnalysisException {
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
  }

  /**
   * Determine if this expression must preserve type because either the
   * parent or any child wants to preserve type. Then, analyze the child with the
   * resulting type preservation decision.
   *
   * 2 + 3 --> 5:TINYINT
   * tinyint_col + 4 --> SMALLINT
   *
   * For pure-literal expressions, use the natural type of the result.
   * Pure-literal expression are those without explicit casts. Due to prior
   * folding events, we only need to check one level of children.
   */
  private boolean checkPreserveType(Expr expr) {
    for (int i = 0; i < expr.getChildCount(); i++) {
      if (expr.getChild(i).hasExplicitType()) return true;
    }
    return false;
  }

  /**
   * Finalize analysis for a node. Required before constant-folding
   * and before rewrite validity checks.
   */
  private void finish(Expr expr) {
    // Rewrite may have returned a child already analyzed
    if (expr.isAnalyzed()) return;
    // Compute cost after all other changes are complete
    expr.computeCost();
    expr.analysisDone();
  }

  /**
   * IMPALA-5125: We can't eliminate aggregates as this may change the meaning of the
   * query, for example:
   * 'select if (true, 0, sum(id)) from alltypes' != 'select 0 from alltypes'
   *
   * @param before expression before rewrite
   * @param after expression after rewrite
   * @return true if the rewrite is valid, false if the rewrite is invalid
   * and must be rejected
   */
  private boolean rewriteIsValid(Expr before, Expr after) {
    return after.contains(Expr.isAggregatePredicate()) ||
           !before.contains(Expr.isAggregatePredicate());
  }

 /**
   * Replace a constant Expr with its equivalent LiteralExpr by evaluating the
   * Expr in the BE. Exprs that are already LiteralExprs are not changed.
   *
   * Examples:
   * 1 + 1 + 1 --> 3
   * toupper('abc') --> 'ABC'
   * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
   *
   * @param expr the expression to fold. This function checks if the expression
   * is sutable for folding
   * @param preserveType if folding is done, whether the literal should take the
   * type of the original expression (true) or take its "natural" type (false)
   * @return the original expression if no folding is done, or a literal that
   * is the result of evaluating the constant expression
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
        constantFoldCount_++;
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

    if (result == null) return expr;
    constantFoldCount_++;
    return result;
  }

  /**
   * Analyze an expression with no rewrites. Used for testing and for
   * temporary backward-compatibility with some older code not yet converted
   * to the new system.
   */
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
  public int constantFoldCount() { return constantFoldCount_; }
  public int transformCount() { return rewriteCount_ + constantFoldCount_; }
}
