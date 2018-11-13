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

package org.apache.impala.common;

import org.apache.impala.analysis.Expr;

/**
 * Thrown for errors encountered during analysis of a SQL statement.
 *
 */
@SuppressWarnings("serial")
public class AnalysisException extends ImpalaException {

  public static final String NOT_SUPPORTED_MSG =
      "%s are not supported in the %s: %s";
  public static final String AGG_FUNC_MSG = "Aggregate functions";
  public static final String ANALYTIC_EXPRS_MSG = "Analytic expressions";
  public static final String SUBQUERIES_MSG = "Subqueries";
  public static final String SELECT_LIST_MSG = "SELECT list";
  public static final String WHERE_CLAUSE_MSG = "WHERE clause";
  public static final String GROUP_BY_CLAUSE_MSG = "GROUP BY clause";
  public static final String ON_CLAUSE_MSG = "ON clause";
  public static final String ORDER_BY_CLAUSE_MSG = "ORDER BY clause";

  public AnalysisException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public AnalysisException(String msg) {
    super(msg);
  }

  public AnalysisException(Throwable cause) {
    super(cause);
  }

  public static AnalysisException notSupported(String feature,
      String clause, String exprSql) {
    return new AnalysisException(
        String.format(NOT_SUPPORTED_MSG, feature, clause, exprSql));
  }

  public static AnalysisException notSupported(String feature,
      String clause, Expr expr) {
    return notSupported(feature, clause, expr.toSql());
  }
}
