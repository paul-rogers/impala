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
import org.apache.impala.common.serialize.JsonSerializable;
import org.apache.impala.common.serialize.ObjectSerializer;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Predicates;

/**
 * Represents a single GROUP BY entry, which can be an ordinal, an alias
 * or an expression.
 */
public class GroupByExpression extends AbstractExpression implements JsonSerializable {
  Expr original_;
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
    // Show the rewritten expression in error message since substitution
    // may have resulted in an illegal expression.
    if (expr_.contains(Expr.isAggregatePredicate())) {
      throw new AnalysisException(
          "GROUP BY expression must not contain aggregate functions: "
              + expr_.toSql());
    }
    if (expr_.contains(AnalyticExpr.class)) {
      throw new AnalysisException(
          "GROUP BY expression must not contain analytic expressions: "
              + expr_.toSql());
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
