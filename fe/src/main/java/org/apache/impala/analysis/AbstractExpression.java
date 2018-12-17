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

/**
 * Represents an expression a while, while the {@link Expr} represents
 * an individual node within the expression tree. An Expr represents the current
 * state of an expression. This class holds both the original "source"
 * expression and the resolved, analyzed, rewritten expression.
 */
// TODO: Move collect here
// TODO: Move getSubquery here
// TODO: Move substitute here
// TODO: Move checkReturnsBool here
public abstract class AbstractExpression {
  protected String sourceSql_;

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
