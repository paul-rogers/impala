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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.serialize.ArraySerializer;
import org.apache.impala.common.serialize.JsonSerializable;
import org.apache.impala.common.serialize.ObjectSerializer;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Predicates;

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
  protected String sourceSql_;

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

    public void analyze(Analyzer analyzer) throws AnalysisException {
      saveSource(expr_);
      expr_ = analyzer.analyzeAndRewrite(expr_);
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

//    public void analyze(Analyzer analyzer) throws AnalysisException {
//      saveSource(preExpansion_);
//      expr_ = analyzer.analyzeAndRewrite(preExpansion_);
//    }

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
      expr_ = stmt.resolveReferenceExpr(preExpansion_, "HAVING", analyzer, false);
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

  public static class GroupByExpression extends AbstractExpression implements JsonSerializable {
    private Expr original_;
    private Expr expr_;

    public GroupByExpression(Expr expr) {
      original_ = expr;
    }

    public void analyze(SelectStmt stmt, Analyzer analyzer) throws AnalysisException {
      saveSource(original_);
      // disallow subqueries in the GROUP BY clause
      if (original_.contains(Predicates.instanceOf(Subquery.class))) {
        throw new AnalysisException(
            "Subqueries are not supported in the GROUP BY clause.");
      }
      expr_ = stmt.resolveReferenceExpr(original_,
          "GROUP BY", analyzer, true);
      if (expr_.contains(Expr.isAggregatePredicate())) {
        // reference the original expr in the error msg
        throw new AnalysisException(
            "GROUP BY expression must not contain aggregate functions: "
                + sourceSql_);
      }
      if (expr_.contains(AnalyticExpr.class)) {
        // reference the original expr in the error msg
        throw new AnalysisException(
            "GROUP BY expression must not contain analytic expressions: "
                + sourceSql_);
      }
    }

    @Override
    public Expr getExpr() { return expr_; }

    public void rewrite(ExprRewriter rewriter, Analyzer analyzer) throws AnalysisException {
      original_ = SelectStmt.rewriteCheckOrdinalResult(
          rewriter, analyzer, original_);
    }

    public void reset() {
      original_.reset();
    }

    @Override
    public void serialize(ObjectSerializer os) {
      expr_.serialize(os);
    }

    @Override
    public String toSql(ToSqlOptions options) {
      if (options == ToSqlOptions.DEFAULT) {
        return sourceSql_ == null ? original_.toSql(options) : sourceSql_;
      } else {
        return expr_ == null ? original_.toSql(options) : expr_.toSql(options);
      }
    }
  }

  public static class GroupByClause {
    private final List<GroupByExpression> groupBy_ = new ArrayList<>();

    public GroupByClause(List<Expr> groupingExprs) {
      append(groupingExprs);
    }

    public void append(List<Expr> groupByExprs) {
      for (Expr expr : groupByExprs)
        groupBy_.add(new GroupByExpression(expr));
    }

    public GroupByClause(GroupByClause from) {
      for (GroupByExpression expr : from.groupBy_)
        groupBy_.add(new GroupByExpression(expr.original_));
    }

    public static GroupByClause wrap(List<Expr> groupingExprs) {
      if (groupingExprs == null || groupingExprs.isEmpty()) return null;
      return new GroupByClause(groupingExprs);
    }

    public void analyze(SelectStmt stmt, Analyzer analyzer) throws AnalysisException {
      for (GroupByExpression expr : groupBy_)
        expr.analyze(stmt, analyzer);
    }

    public List<Expr> getExprs() {
      List<Expr> exprs = new ArrayList<>();
      for (GroupByExpression expr : groupBy_)
        exprs.add(expr.getExpr());
      return exprs;
    }

    public void rewrite(ExprRewriter rewriter, Analyzer analyzer) throws AnalysisException {
      for (GroupByExpression expr : groupBy_)
        expr.rewrite(rewriter, analyzer);
    }

    public String toSql(ToSqlOptions options) {
      StringBuilder strBuilder = new StringBuilder();
      strBuilder.append(" GROUP BY ");
      // Handle both analyzed (multiAggInfo_ != null) and unanalyzed cases.
      // Unanalyzed case is used to generate SQL such as for views.
      // See ToSqlUtils.getCreateViewSql().
      for (int i = 0; i < groupBy_.size(); i++) {
        if (i > 0) strBuilder.append(", ");
        strBuilder.append(groupBy_.get(i).toSql(options));
      }
      return strBuilder.toString();
    }

    public void reset() {
      for (GroupByExpression expr : groupBy_)
        expr.reset();
    }

    public void serialize(ArraySerializer array) {
      for (GroupByExpression expr : groupBy_)
        array.object(expr.getExpr());
    }
  }

  public void saveSource(Expr source) throws AnalysisException {
    // If this is the second analysis pass, keep the existing source
    if (sourceSql_ != null) return;
    // For very deep expressions, the call stack for toSql can blow
    // up: toSql() uses about 2-3 calls per level. In this case,
    // we flag the depth limit exception here, before we even get to
    // the formal check during analysis.
    try {
      sourceSql_ = source.toSql(ToSqlOptions.DEFAULT);
    } catch (StackOverflowError e) {
      throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
          "expression tree: %d", Expr.EXPR_DEPTH_LIMIT));
    }
  }

  public String getSourceExpr() { return sourceSql_; }
  public abstract Expr getExpr();

  public static Expr unwrap(AbstractExpression expression) {
    return expression == null ? null : expression.getExpr();
  }

  public String toSql(ToSqlOptions options) {
    // Enclose aliases in quotes if Hive cannot parse them without quotes.
    // This is needed for view compatibility between Impala and Hive.
    return options == ToSqlOptions.DEFAULT && sourceSql_ != null
        ? sourceSql_ : getExpr().toSql(options);
  }

  @Override
  public String toString() { return getExpr().toString(); }
}
