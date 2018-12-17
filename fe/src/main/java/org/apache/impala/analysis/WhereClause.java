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
 * Represents the (optional) WHERE clause of a SELECT expression.
 * The WHERE clause is a single Boolean predicate.
 */
public class WhereClause extends AbstractExpression {
  protected Expr expr_;

  public WhereClause(Expr expr) {
    expr_ = expr;
  }

  public WhereClause(WhereClause from) {
    expr_ = from.expr_.clone();
  }

  public static WhereClause wrap(Expr expr) {
    return expr == null ? null : new WhereClause(expr);
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    saveSource(expr_);
    expr_ = analyzer.analyzeAndRewrite(expr_);
    if (expr_.contains(Expr.isAggregatePredicate())) {
      throw new AnalysisException(
          "aggregate function not allowed in WHERE clause");
    }
    expr_.checkReturnsBool("WHERE clause", false);
    Expr e = expr_.findFirstOf(AnalyticExpr.class);
    if (e != null) {
      throw new AnalysisException(
          "WHERE clause must not contain analytic expressions: " + e.toSql());
    }
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
  public WhereClause clone() {
    return new WhereClause(this);
  }
}