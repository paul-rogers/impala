package org.apache.impala.analysis;

import static org.junit.Assert.*;

import org.apache.impala.analysis.AnalysisFixture.QueryFixture;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

public class AnalyzerInternalsTest extends FrontendTestBase {

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  private SelectStmt analyze(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return query.selectStmt();
  }

  /**
   * Sanity test of SELECT rewrite
   */
  @Test
  public void testSelect() throws AnalysisException {
    String stmtText =
        "select 1 + 1 + id as c from functional.alltypestiny";
    SelectStmt stmt = analyze(stmtText);
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

    String origSql = "SELECT 1 + 1 + id AS c FROM functional.alltypestiny";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

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
      String stmtText =
          "select (select count(*) + 0 from functional.alltypestiny) as c" +
          " from functional.alltypestiny";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("Subqueries are not supported"));
      // Message contains original expression
      assertTrue(e.getMessage().contains("count(*) + 0"));
    }
  }

  @Test
  public void testAmbiguousAlias() throws AnalysisException {
    String stmtText =
        "SELECT id as c, bool_col as c FROM functional.alltypestiny";
    SelectStmt stmt = analyze(stmtText);

    assertEquals(2, stmt.getSelectList().getItems().size());
    assertEquals(2, stmt.getResultExprs().size());
    assertEquals(2, stmt.getAliasSmap().getLhs().size());
    assertEquals(1, stmt.getAmbiguousAliases().size());
    assertEquals("c", ((SlotRef) stmt.getAmbiguousAliases().get(0)).getLabel());
  }

  /**
   * Sanity test of WHERE rewrite
   */
  @Test
  public void testWhere() throws AnalysisException {
    String stmtText =
        "select id from functional.alltypestiny" +
        " where id = 2 + 1 and true";
    SelectStmt stmt = analyze(stmtText);
    Expr expr = stmt.getWhereClause();

    // Expression should have been rewritten
    assertEquals("id = 3", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, expr.getType());
    assertEquals(6.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution

    String origSql =
        "SELECT id FROM functional.alltypestiny" +
        " WHERE id = 2 + 1 AND TRUE";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should be available when requested
    assertEquals(
        "SELECT id FROM functional.alltypestiny WHERE id = 3",
        stmt.toSql(true));
  }

  /**
   * If WHERE is trivial, discard it, but keep the original
   * for toSql().
   */
  @Test
  public void testTrivialWhere() throws AnalysisException {
    String stmtText = "select id from functional.alltypestiny " +
        "where id = 2 + 1 or true";
    SelectStmt stmt = analyze(stmtText);
    assertNull(stmt.getWhereClause());

    String origSql = "SELECT id FROM functional.alltypestiny " +
        "WHERE id = 2 + 1 OR TRUE";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should have no WHERE clause
    assertEquals("SELECT id FROM functional.alltypestiny",
        stmt.toSql(true));
  }

  /**
   * Analyzer checks for aggregates in the WHERE clause.
   */
  @Test
  public void testWhereAggregate() {
    try {
      String stmtText =
          "select id from functional.alltypestiny" +
          " where count(id) < 10 + 2";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("Aggregate functions are not supported"));
      // Message contains original expression
      assertTrue(e.getMessage().contains("count(id) < 10 + 2"));
    }
  }

  /**
   * WHERE clause, if provided, must be Boolean.
   */
  @Test
  public void testWhereNonBoolean() {
    try {
      String stmtText =
          "select id from functional.alltypestiny" +
          " where 0 + 2 + id";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("requires type BOOLEAN"));
      // Message contains original expression
      assertTrue(e.getMessage().contains("0 + 2 + id"));
    }
  }

  /**
   * WHERE clause cannot contain window functions.
   */
  @Test
  public void testWhereAnalytic() {
    try {
      String stmtText =
          "select id from functional.alltypestiny" +
          " where count(id) over(partition by id / (1 + 3))";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("Analytic expressions are not supported"));
      // Message contains original expression
      assertTrue(e.getMessage().contains("count(id) OVER (PARTITION BY id / (1 + 3))"));
    }
  }


  /**
   * Sanity test of JOIN ... ON rewrite
   */
  @Test
  public void testOnClause() throws AnalysisException {
    String stmtText =
        "select t1.id" +
        " from functional.alltypestiny t1" +
        " join functional.alltypestiny t2" +
        " on 1 + 1 + t1.id = 2 * 1 + t2.id";
    SelectStmt stmt = analyze(stmtText);
    Expr expr = stmt.getFromClause().get(1).getOnClause();

    // Expression should have been rewritten
    assertEquals("2 + t1.id = 2 + t2.id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, expr.getType());
    assertEquals(7.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution

    String origSql =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " JOIN functiona.alltypestiny t2" +
        " ON 1 + 1 + t1.id = 2 * 1 + t2.id";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " JOIN functiona.alltypestiny t2" +
        " ON 2 + t1.id = 2 + t2.id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

}
