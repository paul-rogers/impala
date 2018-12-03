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

/**
 * Represents an expression a while, while the {@link Expr} represents
 * an individual node within the expression tree. An Expr represents the current
 * state of an expression. This class holds both the original "source"
 * expression and the resolved, analyzed, rewritten expression.
 * Also holds properties of the expression a whole.
 */
// TODO: Move collect here
// TODO: Move getSubquery here
// TODO: Move substitute here
// TODO: Move checkReturnsBool here
public abstract class AbstractExpression {
  protected String source_;

  public static class WhereExpression extends AbstractExpression {
    protected Expr expr_;

    public WhereExpression(Expr expr) {
      expr_ = expr;
    }

    public WhereExpression(WhereExpression from) {
      expr_ = from.expr_.clone();
    }

    public static WhereExpression wrap(Expr expr) {
      return expr == null ? null : new WhereExpression(expr);
    }

    @Override
    protected Expr prepare() throws AnalysisException {
      saveSource(expr_);
      return expr_;
    }

    @Override
    public Expr getExpr() { return expr_; }

    public void setExpr(Expr expr) { expr_ = expr; }

    // Temporary
    public void rewrite(ExprRewriter rewriter, Analyzer analyzer) throws AnalysisException {
      expr_ = rewriter.rewrite(expr_, analyzer);
    }

    // Temporary
    public void reset() { expr_.reset(); }

    @Override
    public WhereExpression clone() {
      return new WhereExpression(this);
    }
  }

  /**
   * Temporary shim class to capture HAVING semantics in the two-pass rewrite
   * form.
   */
  public static class HavingExpression extends AbstractExpression {
    // Before rewrites: used to display source SQL
    protected Expr preExpansion_;
    // Current active version
    protected Expr expr_;

    public HavingExpression(Expr expr) {
      preExpansion_ = expr;
    }

    public HavingExpression(HavingExpression from) {
      preExpansion_ = from.preExpansion_;
    }

    public static HavingExpression wrap(Expr expr) {
      return expr == null ? null : new HavingExpression(expr);
    }

    public static Expr unwrap(HavingExpression expression) {
      return expression == null ? null : expression.expr_;
    }

    public Expr getPreExpansion() { return preExpansion_; }

    @Override
    public Expr getExpr() {
      return expr_;
    }

    public void setExpr(Expr expr) {
      expr_ = expr;
    }

     @Override
    public HavingExpression clone() {
      return new HavingExpression(this);
    }

    @Override
    protected Expr prepare() throws AnalysisException {
      saveSource(preExpansion_);
      return preExpansion_;
    }

    public void reset() {
      preExpansion_.reset();
      expr_ = null;
    }

    public void rewrite(ExprRewriter rewriter, Analyzer analyzer) throws AnalysisException {
      // Note: this is very wrong, but is what was historically done.
      // TODO: Replace when rewrites are inline
      preExpansion_ = rewriter.rewrite(preExpansion_, analyzer);
    }

    public void analyze(SelectStmt stmt, Analyzer analyzer) throws AnalysisException {
      expr_ = stmt.resolveReferenceExpr(prepare(), "HAVING", analyzer, false);
    }

    public void substitute(ExprSubstitutionMap combinedSmap, Analyzer analyzer) {
      expr_ = expr_.substitute(combinedSmap, analyzer, false);
    }

    @Override
    public String toSql(ToSqlOptions options) {
      // Choose pre-substitution or post-substitution version depending
      // on type of SQL desired.
      Expr having = options == ToSqlOptions.DEFAULT
          ? getPreExpansion() : getExpr();
      return having.toSql(options);
    }
  }

  public void saveSource(Expr source) throws AnalysisException {
    // If this is the second analysis pass, keep the existing source
    if (source_ != null) return;
    // For very deep expressions, the call stack for toSql can blow
    // up: toSql() uses about 2-3 calls per level. In this case,
    // we flag the depth limit exception here, before we even get to
    // the formal check during analysis.
    try {
      source_ = source.toSql(ToSqlOptions.DEFAULT);
    } catch (StackOverflowError e) {
      throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
          "expression tree: %d", Expr.EXPR_DEPTH_LIMIT));
    }
  }

  public final void analyze(Analyzer analyzer) throws AnalysisException {
    ExprAnalyzer exprAnalyzer = new ExprAnalyzer(analyzer);
    exprAnalyzer.analyze(prepare());
  }

  public String getSourceExpr() { return source_; }
  public abstract Expr getExpr();

  /**
   * Temporary shim to prepare the expression to be used for analysis. With
   * the current two-step analysis, chooses the version of the expression
   * to rewrite. To be replaced later with the final version.
   * TODO: Revisit
   * @throws AnalysisException
   */
  protected abstract Expr prepare() throws AnalysisException;

  public static Expr unwrap(AbstractExpression expression) {
    return expression == null ? null : expression.getExpr();
  }

  public String toSql(ToSqlOptions options) {
    // Enclose aliases in quotes if Hive cannot parse them without quotes.
    // This is needed for view compatibility between Impala and Hive.
    return options == ToSqlOptions.DEFAULT && source_ != null
        ? source_ : getExpr().toSql(options);
  }

  @Override
  public String toString() { return getExpr().toString(); }
}
