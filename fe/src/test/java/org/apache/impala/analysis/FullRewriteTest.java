package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
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


  public ParseNode analyzeExpr(String exprStr) {
    String stmt = "select " + exprStr + " from " + "functional.alltypessmall";
    System.out.println(stmt);
    return analyzeStmt(stmt);
  }

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

  @Test
  public void testSqlRecovery() {
    ParseNode node = analyzeExpr("true and false");
    System.out.println(node.toSql());
    System.out.println(((SelectStmt) node).toSql(true));
    // SELECT TRUE AND FALSE FROM functional.alltypessmall
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

    // IMPALA-7766
    verifySelectRewrite("id + 1 + 2", "id + 1 + 2");

    // IMPALA-7769
    verifySelectRewrite("cast(null as INT) IS NULL", "TRUE");
    verifySelectRewrite("(null + 1) is null", "TRUE");
    verifySelectRewrite("(1 + 1) is null", "FALSE");
    verifySelectRewrite("CASE WHEN null + 1 THEN 10 ELSE 20 END", "20");
  }

  @Test
  public void testIf() throws ImpalaException {

    // Simplifications provided by CASE rewriting
    verifySelectRewrite("if(true, id, id+1)", "id");
    verifySelectRewrite("if(false, id, id+1)", "id + 1");
    verifySelectRewrite("if(null, id, id+1)", "id + 1");
    // Nothing to simplify
    verifySelectRewrite("if(id = 0, true, false)",
        "CASE WHEN id = 0 THEN TRUE ELSE FALSE END");
    // Don't simplify if drops last aggregate
    verifySelectRewrite("if(true, 0, sum(id))",
        "CASE WHEN TRUE THEN 0 ELSE sum(id) END");
    verifySelectRewrite("if(false, sum(id), 0)",
        "CASE WHEN FALSE THEN sum(id) ELSE 0 END");
    // OK to simplify if retains an aggregate
    verifySelectRewrite("if(false, max(id), min(id))", "min(id)");
    verifySelectRewrite("if(true, max(id), min(id))", "max(id)");
    verifySelectRewrite("if(false, 0, sum(id))", "sum(id)");
    verifySelectRewrite("if(true, sum(id), 0)", "sum(id)");
    // If else is NULL, case should drop it since it is
    // the default
    // IMPALA-7750
    verifySelectRewrite("if(id = 0, 10, null)",
        "CASE WHEN id = 0 THEN 10 END");
  }

  /**
   * IfNull and its aliases are rewritten to use case expressions.
   */
  @Test
  public void testifNull() throws ImpalaException {
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      verifySelectRewrite(f + "(null, id)", "id");
      verifySelectRewrite(f + "(null, null)", "NULL");

      verifySelectRewrite(f + "(1, 2)", "1");
      verifySelectRewrite(f + "(0, id)", "0");

      // non literal constants shouldn't be simplified by the rule
      // IMPALA-7766
      verifySelectRewrite(f + "(id + 1 + 1, 2 + 3)",
          "CASE WHEN id + 1 + 1 IS NULL THEN 5 ELSE id + 1 + 1 END");

      // Constant-folding applied
      verifySelectRewrite(f + "(1 + 1, id)", "2");
      verifySelectRewrite(f + "(NULL + 1, id)", "id");

      // Can't simplify if removes only aggregate
      verifySelectRewrite(f + "(null, max(id))", "max(id)");
      verifySelectRewrite(f + "(1, max(id))",
          "CASE WHEN FALSE THEN max(id) ELSE 1 END");
    }
  }

  @Test
  public void testNvl2() throws ImpalaException {

    // Optimizations
    verifySelectRewrite("nvl2(null, 10, 20)", "20");
    verifySelectRewrite("nvl2(4, 10, 20)", "10");

    // Rewrite preserves an aggregate, so keep
    verifySelectRewrite("nvl2(null, 10, sum(id))", "sum(id)");
    verifySelectRewrite("nvl2(null, avg(id), sum(id))", "sum(id)");
    verifySelectRewrite("nvl2(4, sum(id), 20)", "sum(id)");
    verifySelectRewrite("nvl2(4, sum(id), avg(id))", "sum(id)");

    // Rewrite would lose an aggregate, keep full CASE
    verifySelectRewrite("nvl2(null, sum(id), 20)",
        "CASE WHEN FALSE THEN sum(id) ELSE 20 END");
    verifySelectRewrite("nvl2(4, 10, sum(id))",
        "CASE WHEN TRUE THEN 10 ELSE sum(id) END");
  }

  @Test
  public void testWhere() throws ImpalaException {
    verifyWhereRewrite("null + 1 IS NULL", "TRUE");
    verifyWhereRewrite("null is null", "TRUE");
    verifyWhereRewrite("10 is null", "FALSE");
  }

  @Test
  public void testOrderBy() throws ImpalaException {
    verifyOrderByRewrite("id + (2 + 3)", "id + 5");
    verifyOrderByRewrite("null is null", "TRUE");
    verifyOrderByRewrite("if(true, 10, 20)", "10");
  }
}
