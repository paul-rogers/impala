package org.apache.impala.analysis;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.impala.analysis.AnalysisFixture.QueryFixture;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

/**
 * Test of analyzer internals around expression rewrites. In general,
 * rewrites occur shortly after analysis. Checks of an expression
 * occur before rewrite, and any error messages include the user's
 * original expression. Semantic processing occurs after rewrites
 * and so the rewritten form will appear in the derived structures.
 */
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

    // Statement's toSql should be before rewrites
    String origSql = "SELECT 1 + 1 + id AS c FROM functional.alltypestiny";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    assertEquals("SELECT 2 + id AS c FROM functional.alltypestiny",
        stmt.toSql(true));
  }

  private void expectUnsupported(AnalysisException e, String feature,
      String clause, String exprSql) {
    AnalysisException expected =
        AnalysisException.notSupported(feature, clause, exprSql);
    assertEquals(expected.getMessage(), e.getMessage());
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
      expectUnsupported(e,
          AnalysisException.SUBQUERIES_MSG,
          AnalysisException.SELECT_LIST_MSG,
          "(SELECT count(*) + 0 FROM functional.alltypestiny)");
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

    // Statement's toSql should be before rewrites
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
  public void testAggregateInWhereClause() {
    try {
      String stmtText =
          "select id from functional.alltypestiny" +
          " where count(id) < 10 + 2";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.AGG_FUNC_MSG,
          AnalysisException.WHERE_CLAUSE_MSG,
          "count(id) < 10 + 2");
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
      expectRequiresBoolean(e, "0 + 2 + id");
    }
  }

  private void expectRequiresBoolean(AnalysisException e, String exprSql) {
    assertTrue(e.getMessage().contains("requires type BOOLEAN"));
    // Message contains original expression
    assertTrue(e.getMessage().contains(exprSql));
  }

  /**
   * WHERE clause cannot contain window functions.
   */
  @Test
  public void testAnalyticInWhereClause() {
    try {
      String stmtText =
          "select id from functional.alltypestiny" +
          " where count(id) over(partition by id / (1 + 3))";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.ANALYTIC_EXPRS_MSG,
          AnalysisException.WHERE_CLAUSE_MSG,
          "count(id) OVER (PARTITION BY id / (1 + 3))");
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
    assertEquals(15.0, expr.getCost(), 0.1);
    // TODO: Should have accessor
    assertTrue(expr.isOnClauseConjunct_);
    // TODO: Should have accessor
    assertEquals(0, expr.id_.asInt());

    // Rewritten conjunct should be registered. There
    // is only one conjunct, so we just grab that.
    // TODO: Should have accessor
    assertTrue(
        stmt.getAnalyzer().getGlobalState().conjuncts.values().contains(expr));

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " INNER JOIN functional.alltypestiny t2" +
        " ON 1 + 1 + t1.id = 2 * 1 + t2.id";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " INNER JOIN functional.alltypestiny t2" +
        " ON 2 + t1.id = 2 + t2.id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * ON clause cannot contain aggregates.
   */
  @Test
  public void testAggregateOnClause() {
    try {
      String stmtText =
          "select t1.id" +
          " from functional.alltypestiny t1" +
          " join functional.alltypestiny t2" +
          " on 1 + 1 + count(t1.id) = 2 * 1 + count(t2.id)";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.AGG_FUNC_MSG,
          AnalysisException.ON_CLAUSE_MSG,
          "1 + 1 + count(t1.id) = 2 * 1 + count(t2.id)");
    }
  }

  /**
   * ON clause cannot contain window functions.
   */
  @Test
  public void testAnalyticOnClause() {
    try {
      String stmtText =
          "select t1.id" +
          " from functional.alltypestiny t1" +
          " join functional.alltypestiny t2" +
          " on count(t1.id) over(partition by t1.id / (1 + 3)) = t2.id";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.ANALYTIC_EXPRS_MSG,
          AnalysisException.ON_CLAUSE_MSG,
          "count(t1.id) OVER (PARTITION BY t1.id / (1 + 3)) = t2.id");
    }
  }

  /**
   * ON clause cannot contain window functions.
   */
  @Test
  public void testBooleanOnClause() {
    try {
      String stmtText =
          "select t1.id" +
          " from functional.alltypestiny t1" +
          " join functional.alltypestiny t2" +
          " on 1 + 1 + t1.id + t2.id";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectRequiresBoolean(e, "1 + 1 + t1.id + t2.id");
    }
  }

  /**
   * If On clause is trivial, discard it, but keep the original
   * for toSql().
   */
  @Test
  public void testTrivialOn() throws AnalysisException {
    String stmtText =
        "select t1.id" +
        " from functional.alltypestiny t1" +
        " join functional.alltypestiny t2" +
        " on 1 + 1 = 4 - 2";
    SelectStmt stmt = analyze(stmtText);
    assertNull(stmt.getWhereClause());

    stmtText =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " INNER JOIN functional.alltypestiny t2" +
        " ON 1 + 1 = 4 - 2";
    assertEquals(stmtText,  stmt.toSql());
    assertEquals(stmtText,  stmt.toSql(false));

    // Rewritten should have no ON clause
    // TODO: Should the plan include a CROSS JOIN?
    stmtText =
        "SELECT t1.id" +
        " FROM functional.alltypestiny t1" +
        " INNER JOIN functional.alltypestiny t2";
    assertEquals(stmtText, stmt.toSql(true));
  }

  /**
   * Sanity test of GROUP BY rewrite
   */
  @Test
  public void testGroupBy() throws AnalysisException {
    String stmtText =
        "select 2 + int_col, 2 + 3 + id, count(*)" +
        " from functional.alltypestiny" +
        " group by 1 + 1 + int_col, 2 + 3 + id";

    // Above will pass muster because both SELECT and
    // GROUP BY are rewritten before we match up clauses.
    // Note that one SELECT expr is in rewritten form, the
    // other is in non-rewritten form.
    SelectStmt stmt = analyze(stmtText);
    List<Expr> groupBy = stmt.getGroupByClause();

    // Expressions should have been rewritten
    assertEquals("2 + int_col", groupBy.get(0).toSql());
    assertEquals("5 + id", groupBy.get(1).toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(groupBy.get(0).isAnalyzed());
    assertEquals(ScalarType.BIGINT, groupBy.get(0).getType());
    assertEquals(7.0, groupBy.get(0).getCost(), 0.1);
    assertTrue(groupBy.get(1).isAnalyzed());
    assertEquals(ScalarType.BIGINT, groupBy.get(1).getType());
    assertEquals(7.0, groupBy.get(1).getCost(), 0.1);

    // Statement's toSql should be before substitution

    String origSql =
        "SELECT 2 + int_col, 2 + 3 + id, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY 1 + 1 + int_col, 2 + 3 + id";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT 2 + int_col, 5 + id, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY 2 + int_col, 5 + id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testGroupByConstExpr() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from functional.alltypestiny" +
        " group by 1 + 1";

    SelectStmt stmt = analyze(stmtText);
    List<Expr> groupBy = stmt.getGroupByClause();
    Expr expr = groupBy.get(0);

    // Expressions should have been rewritten
    assertEquals("2", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.SMALLINT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY 1 + 1";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    // Note that after rewrite, constant folding has
    // occurred, but is OK because the ordinal check was
    // done before the rewrite.
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY 2";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * Sanity test of GROUP BY 1
   * (Group by an ordinal)
   */
  @Test
  public void testGroupByOrdinal() throws AnalysisException {
    String stmtText =
        "select id, count(*)" +
        " from functional.alltypestiny" +
        " group by 1";

    SelectStmt stmt = analyze(stmtText);
    List<Expr> groupBy = stmt.getGroupByClause();
    Expr expr = groupBy.get(0);

    // Expressions should have been rewritten
    assertEquals("id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.INT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution
    String origSql =
        "SELECT id, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY 1";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * Sanity test of ORDER BY 1
   * (Order by an ordinal)
   */
  @Test
  public void testGroupByAlias() throws AnalysisException {
    String stmtText =
        "select id as c, count(*)" +
        " from functional.alltypestiny" +
        " group by c";

    SelectStmt stmt = analyze(stmtText);
    List<Expr> groupBy = stmt.getGroupByClause();
    Expr expr = groupBy.get(0);

    // Expressions should have been rewritten
    assertEquals("id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.INT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution
    String origSql =
        "SELECT id AS c, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY c";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id AS c, count(*)" +
        " FROM functional.alltypestiny" +
        " GROUP BY id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * GROUP BY clause cannot contain aggregates.
   */
  @Test
  public void testAggregateInGroupBy() {
    try {
      String stmtText =
          "select 2 + count(*)" +
          " from functional.alltypestiny" +
          " group by 1 + 1 + count(*)";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.AGG_FUNC_MSG,
          AnalysisException.GROUP_BY_CLAUSE_MSG,
          "1 + 1 + count(*)");
    }
  }

  /**
   * GROUP BY clause cannot contain window functions.
   */
  @Test
  public void testAnalyticInGroupBy() {
    try {
      String stmtText =
          "select count(id)" +
          " from functional.alltypestiny" +
          " group by count(id) over(partition by id / (1 + 3))";
      analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.ANALYTIC_EXPRS_MSG,
          AnalysisException.GROUP_BY_CLAUSE_MSG,
          "count(id) OVER (PARTITION BY id / (1 + 3))");
    }
  }

  /**
   * GROUP BY cannot contain subqueries.
   */
  @Test
  public void testSubqueryInGroupBy() {
    try {
      String stmtText =
          "select count(id)" +
          " from functional.alltypestiny" +
          " group by (select count(*) + 0 from functional.alltypestiny)";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.SUBQUERIES_MSG,
          AnalysisException.GROUP_BY_CLAUSE_MSG,
          "(SELECT count(*) + 0 FROM functional.alltypestiny)");
    }
  }

  /**
   * GROUP BY 0
   * (ordinals start at 1)
   */
  @Test
  public void testGroupByZeroOrdinal() {
    try {
      String stmtText =
          "select id as c from functional.alltypestiny" +
          " group by 0";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals("GROUP BY: ordinal must be >= 1: 0", e.getMessage());
    }
  }

  @Test
  public void testGroupByOrdinalOutOfRange() {
    try {
      String stmtText =
          "select id as c from functional.alltypestiny" +
          " group by 2";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "GROUP BY: ordinal exceeds the number of items in SELECT list: 2",
          e.getMessage());
    }
  }

  @Test
  public void testGroupByAmbiguousAlias() {
    try {
      String stmtText =
          "select id as c, int_col as c" +
          " from functional.alltypestiny" +
          " group by c";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "GROUP BY: ambiguous column: c",
          e.getMessage());
    }
  }

  @Test
  public void testGroupByUnknownColumn() {
    try {
      String stmtText =
          "select id as c" +
          " from functional.alltypestiny" +
          " group by d";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "Could not resolve column/field reference: 'd'",
          e.getMessage());
    }
  }

  /**
   * Sanity test of ORDER BY rewrite
   */
  @Test
  public void testOrderByExpr() throws AnalysisException {
    String stmtText =
        "select id from functional.alltypestiny" +
        " order by 1 + 1 + id";

    SelectStmt stmt = analyze(stmtText);
    SortInfo sortInfo = stmt.getSortInfo();
    Expr expr = sortInfo.getSortExprs().get(0);

    // Expressions should have been rewritten
    assertEquals("2 + id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BIGINT, expr.getType());
    assertEquals(4.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY 1 + 1 + id ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY 2 + id ASC";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testOrderByConstExpr() throws AnalysisException {
    String stmtText =
        "select id from functional.alltypestiny" +
        " order by 1 + 1";

    SelectStmt stmt = analyze(stmtText);
    SortInfo sortInfo = stmt.getSortInfo();
    Expr expr = sortInfo.getSortExprs().get(0);

    // Expressions should have been rewritten
    assertEquals("2", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.TINYINT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY 1 + 1 ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    // Note that after rewrite, constant folding has
    // occurred, but is OK because the ordinal check was
    // done before the rewrite.
    String rewrittenSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY 2 ASC";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * Sanity test of ORDER BY 1
   * (Order by an ordinal)
   */
  @Test
  public void testOrderByOrdinal() throws AnalysisException {
    String stmtText =
        "select id from functional.alltypestiny" +
        " order by 1";

    SelectStmt stmt = analyze(stmtText);
    SortInfo sortInfo = stmt.getSortInfo();
    Expr expr = sortInfo.getSortExprs().get(0);

    // Expressions should have been rewritten
    assertEquals("id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.INT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution
    String origSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY 1 ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id FROM functional.alltypestiny" +
        " ORDER BY id ASC";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * Sanity test of ORDER BY 1
   * (Order by an ordinal)
   */
  @Test
  public void testOrderByAlias() throws AnalysisException {
    String stmtText =
        "select id as c from functional.alltypestiny" +
        " order by c";

    SelectStmt stmt = analyze(stmtText);
    SortInfo sortInfo = stmt.getSortInfo();
    Expr expr = sortInfo.getSortExprs().get(0);

    // Expressions should have been rewritten
    assertEquals("id", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.INT, expr.getType());
    assertEquals(1.0, expr.getCost(), 0.1);

    // Statement's toSql should be before substitution
    String origSql =
        "SELECT id AS c FROM functional.alltypestiny" +
        " ORDER BY c ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id AS c FROM functional.alltypestiny" +
        " ORDER BY id ASC";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * ORDER BY cannot contain subqueries.
   */
  @Test
  public void testSubqueryInOrderBy() {
    try {
      String stmtText =
          "select id" +
          " from functional.alltypestiny" +
          " order by (select count(*) + 0 from functional.alltypestiny)";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.SUBQUERIES_MSG,
          AnalysisException.ORDER_BY_CLAUSE_MSG,
          "(SELECT count(*) + 0 FROM functional.alltypestiny)");
    }
  }

  /**
   * ORDER BY 0
   * (ordinals start at 1)
   */
  @Test
  public void testOrderByZeroOrdinal() {
    try {
      String stmtText =
          "select id as c from functional.alltypestiny" +
          " order by 0";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals("ORDER BY: ordinal must be >= 1: 0", e.getMessage());
    }
  }

  @Test
  public void testOrderByOrdinalOutOfRange() {
    try {
      String stmtText =
          "select id as c from functional.alltypestiny" +
          " order by 2";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "ORDER BY: ordinal exceeds the number of items in SELECT list: 2",
          e.getMessage());
    }
  }

  @Test
  public void testOrderByAmbiguousAlias() {
    try {
      String stmtText =
          "select id as c, int_col as c" +
          " from functional.alltypestiny" +
          " order by c";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "ORDER BY: ambiguous column: c",
          e.getMessage());
    }
  }

  @Test
  public void testOrderByUnknownColumn() {
    try {
      String stmtText =
          "select id as c" +
          " from functional.alltypestiny" +
          " order by d";
       analyze(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "Could not resolve column/field reference: 'd'",
          e.getMessage());
    }
  }

  // TODO: HAVING
}
