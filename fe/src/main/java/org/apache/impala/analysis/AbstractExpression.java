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

import com.google.common.base.Preconditions;

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
  protected final Expr source_;

  public static class Expression extends AbstractExpression {
    protected Expr expr_;

    public Expression(Expr expr) {
      super(expr.clone());
      expr_ = expr;
    }

    public Expression(Expression from) {
      // No need to clone the source: it is immutable
      super(from.source_);
      expr_ = from.expr_.clone();
    }

    public static Expression wrap(Expr expr) {
      return expr == null ? null : new Expression(expr);
    }

    @Override
    protected Expr prepare() { return expr_; }

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
    public Expression clone() {
      return new Expression(this);
    }
  }

  public static class HavingExpression extends AbstractExpression {
    protected Expr preExpansion_;
    protected Expr expr_;

    public HavingExpression(Expr expr) {
      super(expr.clone());
      preExpansion_ = expr;
    }

    public HavingExpression(HavingExpression from) {
      super(from.source_);
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
    protected Expr prepare() {
      assert false;
      return null;
    }

    public void reset() {
      preExpansion_.reset();
      expr_ = null;
    }

     public void rewrite(ExprRewriter rewriter, Analyzer analyzer) throws AnalysisException {
      // Note: this is very wrong, but is what was historically done.
      // TODO: Replace when rewrites are inline
      expr_ = SelectStmt.rewriteCheckOrdinalResult(rewriter, analyzer, preExpansion_);
    }
  }

  public AbstractExpression(Expr source) {
    source_ = source;
  }

  public final void analyze(Analyzer analyzer) throws AnalysisException {
    ExprAnalyzer exprAnalyzer = new ExprAnalyzer(analyzer);
    exprAnalyzer.analyze(prepare());
  }

  public Expr getSourceExpr() { return source_; }
  public abstract Expr getExpr();

  /**
   * Temporary shim to prepare the expression to be used for analysis. With
   * the current two-step analysis, chooses the version of the expression
   * to rewrite. To be replaced later with the final version.
   * TODO: Revisit
   */
  protected abstract Expr prepare();

  public static Expr unwrap(AbstractExpression expression) {
    return expression == null ? null : expression.getExpr();
  }

  public String toSql(ToSqlOptions options) {
    // Enclose aliases in quotes if Hive cannot parse them without quotes.
    // This is needed for view compatibility between Impala and Hive.
    return options == ToSqlOptions.DEFAULT && source_ != null
        ? source_.toSql() : getExpr().toSql(options);
  }

  @Override
  public String toString() { return getExpr().toString(); }
}
