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

import static org.junit.Assert.assertEquals;

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Several of the conditional functions count on the CASE operator
 * to simplify their expressions. This test runs the full analyze step,
 * repeatedly running rules, to test the compound rewrites of first
 * handling the conditional functions, then further simplifying the
 * resulting CASE expression.
 *
 * No tests are done for coalesce since its rewrite handles all the
 * required simplification.
 */
public class FullRewriteTest extends FrontendTestBase {

  public ParseNode analyzeStmt(String stmt) {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryCtx().client_request.query_options.setEnable_expr_rewrites(true);
    return AnalyzesOk(stmt, ctx, null);
  }

  public Expr verifySelectRewrite(String exprStr, String expectedExprStr)
      throws ImpalaException {
    return verifySelectRewrite("functional.alltypessmall", exprStr, expectedExprStr);
  }

  private Expr verifySelectRewrite(String tableName, String exprStr,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select " + exprStr + " from " + tableName;
    return verifySelectStmtRewrite(stmtStr, expectedExprStr);
  }

  private Expr verifySelectStmtRewrite(String stmtStr, String expectedExprStr) {
    SelectStmt stmt = (SelectStmt) analyzeStmt(stmtStr);
    Expr rewrittenExpr = stmt.getSelectList().getItems().get(0).getExpr();
    String rewrittenSql = rewrittenExpr.toSql();
    assertEquals(expectedExprStr, rewrittenSql);
    return rewrittenExpr;
  }

  public Expr verifyWhereRewrite(String exprStr, String expectedExprStr)
      throws ImpalaException {
    return verifyWhereRewrite("functional.alltypessmall", exprStr, expectedExprStr);
  }

  private Expr verifyWhereRewrite(String tableName, String exprStr,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select id from " + tableName + " where " + exprStr;
    SelectStmt stmt = (SelectStmt) analyzeStmt(stmtStr);
    Expr rewrittenExpr = stmt.getWhereClause();
    String rewrittenSql = rewrittenExpr.toSql();
    assertEquals(expectedExprStr, rewrittenSql);
    return rewrittenExpr;
  }

  public Expr verifyOrderByRewrite(String exprStr, String expectedExprStr)
      throws ImpalaException {
    return verifyOrderByRewrite("functional.alltypessmall", exprStr, expectedExprStr);
  }

  private Expr verifyOrderByRewrite(String tableName, String exprStr,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select id from " + tableName + " order by " + exprStr;
    SelectStmt stmt = (SelectStmt) analyzeStmt(stmtStr);
    Expr rewrittenExpr = stmt.getSortInfo().getSortExprs().get(0);
    String rewrittenSql = rewrittenExpr.toSql();
    assertEquals(expectedExprStr, rewrittenSql);
    return rewrittenExpr;
  }

  private Expr verifyGroupByRewrite(String stmtStr,
      String expectedExprStr) throws ImpalaException {
    SelectStmt stmt = (SelectStmt) analyzeStmt(stmtStr);
    Expr rewrittenExpr = stmt.getGroupingExprs().get(0);
    String rewrittenSql = rewrittenExpr.toSql();
    assertEquals(expectedExprStr, rewrittenSql);
    return rewrittenExpr;
  }

  /**
   * Test some basic simplifications that are assumed in the
   * subsequent tests. These uncovered subtle errors and are here
   * to prevent regressions.
   */
  @Test
  public void sanityTest() throws ImpalaException {
    verifySelectRewrite("null + 1", "NULL");
    verifySelectRewrite("null is null", "TRUE");
    verifySelectRewrite("id + (2 + 3)", "id + 5");
    verifySelectRewrite("1 + 2 + id", "3 + id");
  }

  @Test
  public void testWhere() throws ImpalaException {
    verifyWhereRewrite("null + 1 IS NULL", "TRUE");
    verifyWhereRewrite("null is null", "TRUE");
    verifyWhereRewrite("10 is null", "FALSE");
  }

  @Test
  public void testOrderBy() throws ImpalaException {
//    verifyOrderByRewrite("null + 1 IS NULL", "TRUE");
//    verifyOrderByRewrite("null is null", "TRUE");
//    verifyOrderByRewrite("10 is null", "FALSE");
    verifyOrderByRewrite("concat('foo', 'bar')", "'foobar'");
    verifyOrderByRewrite("CASE WHEN TRUE THEN (CAST 10 AS INT) WHEN FALSE THEN 20 ELSE 30 END", "10");
    verifyOrderByRewrite("id + (2 + 3)", "id + 5");
    verifyOrderByRewrite("null is null", "TRUE");
    verifyOrderByRewrite("if(true, 10, 20)", "10");
  }

  @Test
  public void testGroupBy() throws ImpalaException {
    // Analyzer can handle rewrites in second level of
    // the GROUP BY clause
    String stmt =
        "SELECT int_col + (1 + 2)\n" +
        "FROM functional.alltypes\n" +
        "GROUP BY int_col + (1 + 2)";
    verifyGroupByRewrite(stmt,
        "int_col + 3");

    // The code rewrites the SELECT clause...
    verifySelectRewrite("1 + 2", "3");

    // But not the top level of the GROUP BY clause
    // IMPALA-7785: GROUP BY clause not analyzed prior to rewrite step
    stmt =
        "SELECT 1 + 2\n" +
        "FROM functional.alltypes\n" +
        "GROUP BY 1 + 2";
    verifyGroupByRewrite(stmt, "3");

    // Can get work around IMPALA-7785 by manually rewriting
    // the GROUP BY clause
    stmt =
        "SELECT true && id = 0\n" +
        "FROM functional.alltypes\n" +
        "GROUP BY id = 0";
    verifyGroupByRewrite(stmt, "id = 0");

    // But not by counting on the rewrite engine to do so.
    stmt =
        "SELECT id = 0 && true\n" +
        "FROM functional.alltypes\n" +
        "GROUP BY id = 0 && true";
    verifyGroupByRewrite(stmt, "id = 0");
  }

 }
