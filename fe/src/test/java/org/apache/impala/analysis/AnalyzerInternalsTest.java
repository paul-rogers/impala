package org.apache.impala.analysis;

import static org.junit.Assert.*;

import org.apache.impala.analysis.AnalysisFixture.QueryFixture;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

public class AnalyzerInternalsTest extends FrontendTestBase {

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  /**
   * Sanity test of SELECT rewrite
   */
  @Test
  public void testSelect() throws AnalysisException {
    String stmtText = "SELECT 1 + 1 + id as c FROM functional.alltypestiny";
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    SelectStmt stmt = query.selectStmt();
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

    // Statement's toSql should be before substitution

    assertEquals("SELECT 1 + 1 + id AS c FROM functional.alltypestiny",
        stmt.toSql());
    assertEquals("SELECT 1 + 1 + id AS c FROM functional.alltypestiny",
        stmt.toSql(false));

    // Rewritten should be available when requested
    assertEquals("SELECT 2 + id AS c FROM functional.alltypestiny",
        stmt.toSql(true));
  }

  /**
   * Analyzer checks for nested subqueries in the SELECT clause.
   */
  @Test
  public void testSelectSubquery() {
    try {
      String stmtText = "SELECT (SELECT count(*) FROM functional.alltypestiny) as c" +
          " FROM functional.alltypestiny";
      QueryFixture query = fixture.query(stmtText);
      query.analyze();
      fail();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("Subqueries are not supported"));
    }
  }

  @Test
  public void testAmbiguousAlias() throws AnalysisException {
    String stmtText = "SELECT id as c, bool_col as c FROM functional.alltypestiny";
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    SelectStmt stmt = query.selectStmt();

    assertEquals(2, stmt.getSelectList().getItems().size());
    assertEquals(2, stmt.getResultExprs().size());
    assertEquals(2, stmt.getAliasSmap().getLhs().size());
    assertEquals(1, stmt.getAmbiguousAliases().size());
    assertEquals("c", ((SlotRef) stmt.getAmbiguousAliases().get(0)).getLabel());
  }
}
