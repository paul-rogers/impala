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

    // TODO: IMPALA-7766
//    verifySelectRewrite("id + 1 + 2", "id + 3");

    // TODO: IMPALA-7769
//    verifySelectRewrite("cast(null as INT) IS NULL", "TRUE");
//    verifySelectRewrite("(null + 1) is null", "TRUE");
//    verifySelectRewrite("(1 + 1) is null", "FALSE");
//    verifySelectRewrite("CASE WHEN null + 1 THEN 10 ELSE 20 END", "20");
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
//    verifySelectRewrite("if(id = 0, 10, null)",
//        "CASE WHEN id = 0 THEN 10 END");
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

      // Should simplify constants within an expression
      // IMPALA-7766
//      verifySelectRewrite(f + "(id + 1 + 1, 2 + 3)",
//          "CASE WHEN id + 2 IS NULL THEN 5 ELSE id + 1 + 1 END");

      // Constant-folding applied
      verifySelectRewrite(f + "(1 + 1, id)", "2");
      // TODO: IMPALA-7750
//      verifySelectRewrite(f + "(NULL + 1, id)", "id");

      // Can't simplify if removes only aggregate
      verifySelectRewrite(f + "(null, max(id))", "max(id)");
      verifySelectRewrite(f + "(1, max(id))",
          "CASE WHEN FALSE THEN max(id) ELSE 1 END");
    }
  }

  @Test
  public void testNvl2() throws ImpalaException {

    verifySelectRewrite("nvl2(null, 10, 20)", "20");
    verifySelectRewrite("nvl2(4, 10, 20)", "10");

    // Rewrite preserves an aggregate.
    verifySelectRewrite("nvl2(null, 10, sum(id))", "sum(id)");
    verifySelectRewrite("nvl2(4, sum(id), 20)", "sum(id)");
    verifySelectRewrite("nvl2(4, sum(id), avg(id))", "sum(id)");

    // Rewrite would lose an aggregate, keep full CASE
    verifySelectRewrite("nvl2(null, sum(id), 20)",
        "CASE WHEN FALSE THEN sum(id) ELSE 20 END");
    verifySelectRewrite("nvl2(4, 10, sum(id))",
        "CASE WHEN TRUE THEN 10 ELSE sum(id) END");
  }

  @Test
  public void TestSimplifyDistinctFromRule() throws ImpalaException {
    verifySelectRewrite("if(bool_col is distinct from bool_col, 1, 2)", "2");
    verifySelectRewrite("if(bool_col is not distinct from bool_col, 1, 2)", "1");
    verifySelectRewrite("if(bool_col <=> bool_col, 1, 2)", "1");
    verifySelectRewrite("if(bool_col <=> NULL, 1, 2)",
        "CASE WHEN bool_col IS NOT DISTINCT FROM NULL THEN 1 ELSE 2 END");
  }

  /**
   * NULLIF gets converted to an IF, and has cases where
   * it can be further simplified via SimplifyDistinctFromRule.
   */
  @Test
  public void TestNullif() throws ImpalaException {
    // nullif: converted to if and simplified
    verifySelectRewrite("nullif(bool_col, bool_col)", "NULL");

    // works because the expression tree is identical;
    // more complicated things like nullif(int_col + 1, 1 + int_col)
    // are not simplified
    verifySelectRewrite("nullif(1 + int_col, 1 + int_col)", "NULL");
  }

  /**
   * Test nullif(expr1, expr2).
   *
   * Returns NULL if the two specified arguments are equal. If the specified
   * arguments are not equal, returns the value of expr1.
   */
  @Test
  public void testNullIf() throws ImpalaException {
    // Nothing is equal to null, so return expr1, which is NULL.
    // TODO: IMPALA-7750
//    verifySelectRewrite("nullif(null, id)", "NULL");
    // Nothing is equal to null, so return expr1, which is id.
    // TODO: IMPALA-7750
//    verifySelectRewrite("nullif(id, null)", "id");
    // Otherwise, rewrite to a CASE statement.
    // TODO: IMPALA-7750
//    verifySelectRewrite("nullif(id, 10)",
//        "CASE WHEN id IS DISTINCT FROM 10 THEN id END");
    verifySelectRewrite("nullif(id, 10)",
        "CASE WHEN id IS DISTINCT FROM 10 THEN id ELSE NULL END");
  }
}
