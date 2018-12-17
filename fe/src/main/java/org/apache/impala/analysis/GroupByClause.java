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
import org.apache.impala.rewrite.ExprRewriter;

/**
 * Represents the GROUP BY clause of a SELECT statement: a list
 * of expressions.
 */
public class GroupByClause {
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
