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

import com.google.common.base.Predicates;

/**
 * Represents the Having clause of a SELECT statement.
 */
public class HavingClause extends AbstractExpression {
  // Before rewrites: used to display source SQL
  protected Expr preExpansion_;
  // Current active version
  protected Expr expr_;

  public HavingClause(Expr expr) {
    preExpansion_ = expr;
  }

  public HavingClause(HavingClause from) {
    preExpansion_ = from.preExpansion_;
  }

  public static HavingClause wrap(Expr expr) {
    return expr == null ? null : new HavingClause(expr);
  }

  public static Expr unwrap(HavingClause expression) {
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
  public HavingClause clone() {
    return new HavingClause(this);
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
    // can't contain subqueries
    if (preExpansion_.contains(Predicates.instanceOf(Subquery.class))) {
      throw new AnalysisException(
          "Subqueries are not supported in the HAVING clause.");
    }
    expr_ = stmt.resolveReferenceExpr(preExpansion_, "HAVING", analyzer, false);
    // can't contain analytic exprs
    Expr analyticExpr = expr_.findFirstOf(AnalyticExpr.class);
    if (analyticExpr != null) {
      throw new AnalysisException(
          "HAVING clause must not contain analytic expressions: "
             + analyticExpr.toSql());
    }
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
