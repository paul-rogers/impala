package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

/**
 * Test of analyzer internals around expression rewrites
 * in the SELECT statement. In general,
 * rewrites occur shortly after analysis. Checks of an expression
 * occur before rewrite, and any error messages include the user's
 * original expression. Semantic processing occurs after rewrites
 * and so the rewritten form will appear in the derived structures.
 */
public class SelectRewriteTest extends FrontendTestBase {

  public static final String SELECT_TABLE = "functional.alltypestiny";

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  private SelectStmt analyzeSelect(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return query.selectStmt();
  }

  private void verifyErrorMsg(Exception e, String substr) {
    if (e.getMessage().contains(substr)) {
      return;
    }
    fail("Incorrect error message.\nExpected: .*" +
        substr + ".*\nActual: " + e.getMessage());
  }

  //-----------------------------------------------------------------
  // SELECT list

  /**
   * Sanity test of SELECT list rewrite
   */
  @Test
  public void testSelect() throws AnalysisException {
    String stmtText =
        "select 1 + 1 + id as c from " + SELECT_TABLE;
    SelectStmt stmt = analyzeSelect(stmtText);
    Expr expr = stmt.getSelectList().getItems().get(0).getExpr();

    // Expression should have been rewritten
    assertEquals("2 + id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BIGINT, expr.getType());
    assertEquals(7.0, expr.getCost(), 0.1);

    // Rewritten form should be in the result expressions list
    assertSame(expr, stmt.getResultExprs().get(0));

    // Alias should point to clone of rewritten expression
    Expr lhs = stmt.getAliasSmap().getLhs().get(0);
    assertTrue(lhs instanceof SlotRef);
    assertEquals("c", ((SlotRef) lhs).getLabel());
    Expr rhs = stmt.getAliasSmap().getRhs().get(0);
    assertEquals("2 + id", rhs.toSql());

    // TODO: lhs SlotRef is unanalyzed. Should it be?
    // TODO: rhs is a copy of the SELECT expr. Should it be?

    // Statement's toSql should be before rewrites
    String origSql = "SELECT 1 + 1 + id AS c FROM " + SELECT_TABLE;
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(ToSqlOptions.DEFAULT));

    // Rewritten should be available when requested
    assertEquals("SELECT 2 + id AS c FROM " + SELECT_TABLE,
        stmt.toSql(ToSqlOptions.REWRITTEN));
  }

  /**
   * Analyzer checks for nested subqueries in the SELECT clause.
   */
  @Test
  public void testSelectSubquery() {
    try {
      String stmtText =
          "select (select count(*) + 0 from functional.alltypestiny) as c" +
          " from " + SELECT_TABLE;
      analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      verifyErrorMsg(e, "Subqueries are not supported");
    }
  }

  @Test
  public void testAmbiguousAlias() throws AnalysisException {
    String stmtText =
        "SELECT id as c, bool_col as c FROM " + SELECT_TABLE;
    SelectStmt stmt = analyzeSelect(stmtText);

    assertEquals(2, stmt.getSelectList().getItems().size());
    assertEquals(2, stmt.getResultExprs().size());
    assertEquals(2, stmt.getAliasSmap().getLhs().size());
    assertEquals(1, stmt.getAmbiguousAliases().size());
    assertEquals("c", ((SlotRef) stmt.getAmbiguousAliases().get(0)).getLabel());
  }
}
