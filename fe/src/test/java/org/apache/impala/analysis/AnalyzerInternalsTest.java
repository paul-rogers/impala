package org.apache.impala.analysis;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.impala.analysis.AnalysisFixture.QueryFixture;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.View;
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

  // TODO: Double-check ORDER BY (may have been displaced by GROUP BY)
  // TODO: Test OFFSET and LIMIT
  // TODO: Test SELECT FROM VALUES ...
  // TODO: Test WITH clause

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  private SelectStmt analyzeSelect(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return query.selectStmt();
  }

  //-----------------------------------------------------------------
  // SELECT statement

  public static final String SELECT_TABLE = "functional.alltypestiny";

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
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    assertEquals("SELECT 2 + id AS c FROM " + SELECT_TABLE,
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
          " from " + SELECT_TABLE;
      analyzeSelect(stmtText);
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
        "SELECT id as c, bool_col as c FROM " + SELECT_TABLE;
    SelectStmt stmt = analyzeSelect(stmtText);

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
        "select id from " + SELECT_TABLE +
        " where id = 2 + 1 and true";
    SelectStmt stmt = analyzeSelect(stmtText);
    Expr expr = stmt.getWhereClause();

    // Expression should have been rewritten
    assertEquals("id = 3", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, expr.getType());
    assertEquals(6.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT id FROM " + SELECT_TABLE +
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
    SelectStmt stmt = analyzeSelect(stmtText);
    assertNull(stmt.getWhereClause());

    String origSql = "SELECT id FROM functional.alltypestiny " +
        "WHERE id = 2 + 1 OR TRUE";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should have no WHERE clause
    assertEquals("SELECT id FROM " + SELECT_TABLE,
        stmt.toSql(true));
  }

  /**
   * Analyzer checks for aggregates in the WHERE clause.
   */
  @Test
  public void testAggregateInWhereClause() {
    try {
      String stmtText =
          "select id from " + SELECT_TABLE +
          " where count(id) < 10 + 2";
      analyzeSelect(stmtText);
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
          "select id from " + SELECT_TABLE +
          " where 0 + 2 + id";
      analyzeSelect(stmtText);
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
          "select id from " + SELECT_TABLE +
          " where count(id) over(partition by id / (1 + 3))";
      analyzeSelect(stmtText);
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
    SelectStmt stmt = analyzeSelect(stmtText);
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
      analyzeSelect(stmtText);
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
      analyzeSelect(stmtText);
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
      analyzeSelect(stmtText);
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
    SelectStmt stmt = analyzeSelect(stmtText);
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
        " from " + SELECT_TABLE +
        " group by 1 + 1 + int_col, 2 + 3 + id";

    // Above will pass muster because both SELECT and
    // GROUP BY are rewritten before we match up clauses.
    // Note that one SELECT expr is in rewritten form, the
    // other is in non-rewritten form.
    SelectStmt stmt = analyzeSelect(stmtText);
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
        " FROM " + SELECT_TABLE +
        " GROUP BY 1 + 1 + int_col, 2 + 3 + id";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT 2 + int_col, 5 + id, count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY 2 + int_col, 5 + id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testGroupByConstExpr() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from " + SELECT_TABLE +
        " group by 1 + 1";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        " FROM " + SELECT_TABLE +
        " GROUP BY 1 + 1";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    // Note that after rewrite, constant folding has
    // occurred, but is OK because the ordinal check was
    // done before the rewrite.
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
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
        " from " + SELECT_TABLE +
        " group by 1";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        " FROM " + SELECT_TABLE +
        " GROUP BY 1";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id, count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY id";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * Sanity test of GOUP BY 1
   * (Group by an ordinal)
   */
  @Test
  public void testGroupByAlias() throws AnalysisException {
    String stmtText =
        "select id as c, count(*)" +
        " from " + SELECT_TABLE +
        " group by c";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        " FROM " + SELECT_TABLE +
        " GROUP BY c";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id AS c, count(*)" +
        " FROM " + SELECT_TABLE +
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
          " from " + SELECT_TABLE +
          " group by 1 + 1 + count(*)";
      analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " group by count(id) over(partition by id / (1 + 3))";
      analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " group by (select count(*) + 0 from functional.alltypestiny)";
       analyzeSelect(stmtText);
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
          "select id as c from " + SELECT_TABLE +
          " group by 0";
       analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals("GROUP BY: ordinal must be >= 1: 0", e.getMessage());
    }
  }

  @Test
  public void testGroupByOrdinalOutOfRange() {
    try {
      String stmtText =
          "select id as c from " + SELECT_TABLE +
          " group by 2";
       analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " group by c";
       analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " group by d";
       analyzeSelect(stmtText);
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
        "select id from " + SELECT_TABLE +
        " order by 1 + 1 + id";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        "SELECT id FROM " + SELECT_TABLE +
        " ORDER BY 1 + 1 + id ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id FROM " + SELECT_TABLE +
        " ORDER BY 2 + id ASC";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testOrderByConstExpr() throws AnalysisException {
    String stmtText =
        "select id from " + SELECT_TABLE +
        " order by 1 + 1";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        "SELECT id FROM " + SELECT_TABLE +
        " ORDER BY 1 + 1 ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    // Note that after rewrite, constant folding has
    // occurred, but is OK because the ordinal check was
    // done before the rewrite.
    String rewrittenSql =
        "SELECT id FROM " + SELECT_TABLE +
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
        "select id from " + SELECT_TABLE +
        " order by 1";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        "SELECT id FROM " + SELECT_TABLE +
        " ORDER BY 1 ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id FROM " + SELECT_TABLE +
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
        "select id as c from " + SELECT_TABLE +
        " order by c";

    SelectStmt stmt = analyzeSelect(stmtText);
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
        "SELECT id AS c FROM " + SELECT_TABLE +
        " ORDER BY c ASC";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT id AS c FROM " + SELECT_TABLE +
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
          " from " + SELECT_TABLE +
          " order by (select count(*) + 0 from functional.alltypestiny)";
       analyzeSelect(stmtText);
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
          "select id as c from " + SELECT_TABLE +
          " order by 0";
       analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals("ORDER BY: ordinal must be >= 1: 0", e.getMessage());
    }
  }

  @Test
  public void testOrderByOrdinalOutOfRange() {
    try {
      String stmtText =
          "select id as c from " + SELECT_TABLE +
          " order by 2";
       analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " order by c";
       analyzeSelect(stmtText);
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
          " from " + SELECT_TABLE +
          " order by d";
       analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      assertEquals(
          "Could not resolve column/field reference: 'd'",
          e.getMessage());
    }
  }

  /**
   * Sanity test of HAVING rewrite
   */
  @Test
  public void testHaving() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from " + SELECT_TABLE +
        " group by string_col" +
        " having 1 + 1 + count(*) > 2 + 3";
    SelectStmt stmt = analyzeSelect(stmtText);
    Expr expr = stmt.getHavingPred();

    // Expression should have been rewritten
    assertEquals("2 + count(*) > 5", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, expr.getType());
    assertEquals(5.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 1 + 1 + count(*) > 2 + 3";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 2 + count(*) > 5";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * If HAVING is trivial, it still must be kept.
   * HAVING FALSE -- Return no rows
   * HAVING TRUE -- Return all rows
   */
  @Test
  public void testTrivialTrueHaving() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from " + SELECT_TABLE +
        " group by string_col" +
        " having 1 = 1";
    SelectStmt stmt = analyzeSelect(stmtText);
    Expr expr = stmt.getHavingPred();

    assertNotNull(expr);
    assertTrue(Expr.IS_TRUE_LITERAL.apply(expr));

    String origSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 1 = 1";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should have no WHERE clause
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING TRUE";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testTrivialFalseHaving() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from " + SELECT_TABLE +
        " group by string_col" +
        " having 1 = 2";
    SelectStmt stmt = analyzeSelect(stmtText);
    Expr expr = stmt.getHavingPred();

    assertNotNull(expr);
    assertTrue(Expr.IS_FALSE_LITERAL.apply(expr));

    String origSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 1 = 2";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should have no WHERE clause
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING FALSE";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testAggregateInHavingClause() throws AnalysisException {
    String stmtText =
        "select count(*)" +
        " from " + SELECT_TABLE +
        " group by string_col" +
        " having 1 + 1 + count(*) > 2 + 3";
    SelectStmt stmt = analyzeSelect(stmtText);

    String origSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 1 + 1 + count(*) > 2 + 3";
    assertEquals(origSql,  stmt.toSql());
    assertEquals(origSql,  stmt.toSql(false));

    // Rewritten should have no WHERE clause
    String rewrittenSql =
        "SELECT count(*)" +
        " FROM " + SELECT_TABLE +
        " GROUP BY string_col" +
        " HAVING 2 + count(*) > 5";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  /**
   * HAVING clause, if provided, must be Boolean.
   */
  @Test
  public void testHavingNonBoolean() {
    try {
      String stmtText =
          "select count(*)" +
          " from " + SELECT_TABLE +
          " group by string_col" +
          " having count(*)";
      analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectRequiresBoolean(e, "count(*)");
    }
  }

  /**
   * WHERE clause cannot contain window functions.
   */
  @Test
  public void testAnalyticInHavingClause() {
    try {
      String stmtText =
          "select count(*)" +
          " from " + SELECT_TABLE +
          " group by string_col" +
          " having count(id) OVER (PARTITION BY id / (1 + 3))";
      analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.ANALYTIC_EXPRS_MSG,
          AnalysisException.HAVING_CLAUSE_MSG,
          "count(id) OVER (PARTITION BY id / (1 + 3))");
    }
  }

  /**
   * HAVING cannot contain subqueries.
   */
  @Test
  public void testSubqueryInHaving() {
    try {
      String stmtText =
          "select count(*)" +
          " from " + SELECT_TABLE +
          " group by string_col" +
          " having (select count(*) + 0 from functional.alltypestiny)";
        analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      expectUnsupported(e,
          AnalysisException.SUBQUERIES_MSG,
          AnalysisException.HAVING_CLAUSE_MSG,
          "(SELECT count(*) + 0 FROM functional.alltypestiny)");
    }
  }

  // NOTE: Neither the SQL standard, nor major vendors
  // (PostgreSQL nor RedShift) support ordinals or aliases in the
  // HAVING clause. HAVING is an expression, so ordinals are clearly
  // ambiguous: HAVING 1 = 2 -- Does this mean comparison of two constants,
  // two columns, or a column with a constant? Note that WHERE does not
  // support ordinals either. Aliases could work, but are also not
  // supported in the standard or by major vendors.
  //
  // This note is here because version 3.0 of Impala tried (but failed)
  // to support ordinals and aliases.
  //
  // Hence, there are no tests for these two cases. If there were,
  // they would be here.

  //-----------------------------------------------------------------
  // DELETE statement
  //
  // Delete is only defined for Kudu tables

  public static final String MODIFY_TABLE = "functional_kudu.alltypestiny";

  private DeleteStmt analyzeDelete(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return (DeleteStmt) query.parseNode();
  }

  /**
   * Sanity test of DELETE ... WHERE rewrite
   */
  @Test
  public void testDeleteWhere() throws AnalysisException {
    String stmtText =
        "delete from " + MODIFY_TABLE +
        " where 1 + 1 + id = 8 + 2";
    DeleteStmt stmt = analyzeDelete(stmtText);
    SelectStmt selStmt = ((SelectStmt) stmt.getQueryStmt());
    Expr expr = selStmt.getWhereClause();

    // Expression should have been rewritten
    assertEquals("2 + id = 10", expr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(expr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, expr.getType());
    assertEquals(12.0, expr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "DELETE FROM " + MODIFY_TABLE +
        " WHERE 1 + 1 + id = 8 + 2";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "DELETE FROM " + MODIFY_TABLE +
        " WHERE 2 + id = 10";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }
  //-----------------------------------------------------------------
  // UPDATE statement

  private UpdateStmt analyzeUpdate(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return (UpdateStmt) query.parseNode();
  }

  /**
   * Sanity test of UPDATE ... SET ... rewrite
   */
  @Test
  public void testUpdate() throws AnalysisException {
    String stmtText =
        "UPDATE " + MODIFY_TABLE +
        " set int_col = 2 + 3 + smallint_col" +
        " where 1 + 1 + id = 8 + 2";
    UpdateStmt stmt = analyzeUpdate(stmtText);
    SelectStmt selStmt = ((SelectStmt) stmt.getQueryStmt());
    Expr whereExpr = selStmt.getWhereClause();
    Expr setExpr = stmt.getAssignments().get(0).rhs_;

    // Expression should have been rewritten
    assertEquals("2 + id = 10", whereExpr.toSql());
    assertEquals("5 + smallint_col", setExpr.toSql());

    // Should have been re-analyzed after rewrite
    assertTrue(whereExpr.isAnalyzed());
    assertEquals(ScalarType.BOOLEAN, whereExpr.getType());
    assertEquals(12.0, whereExpr.getCost(), 0.1);

    assertTrue(setExpr.isAnalyzed());
    assertEquals(ScalarType.INT, setExpr.getType());
    assertEquals(7.0, setExpr.getCost(), 0.1);

    // Statement's toSql should be before rewrites
    String origSql =
        "UPDATE " + MODIFY_TABLE +
        " SET int_col = 2 + 3 + smallint_col" +
        " WHERE 1 + 1 + id = 8 + 2";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "UPDATE " + MODIFY_TABLE +
        " SET int_col = 5 + smallint_col" +
        " WHERE 2 + id = 10";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  private void verifyErrorMsg(Exception e, String substr) {
    if (e.getMessage().contains(substr)) {
      return;
    }
    fail("Incorrect error message.\nExpected: .*" +
        substr + ".*\nActual: " + e.getMessage());
  }

  @Test
  public void testSubqueryInUpdate() {
    try {
      String stmtText =
          "UPDATE " + MODIFY_TABLE +
          " set int_col = (select count(*) + 0 from " + SELECT_TABLE + ")" +
          " where 1 + 1 + id = 8 + 2";
        analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      verifyErrorMsg(e, "Subqueries are not supported");
      verifyErrorMsg(e, "(SELECT count(*) + 0 FROM " + SELECT_TABLE + ")");
    }
  }

  // Turns out that there is no way to trigger the check for
  // isBoundByTupleIds; the lhs fails analysis before that check.

  // Also, turns out that the destColumn() check appears to not
  // be reachable for the same reason.

  @Test
  public void testUpdateKeyColumn() {
    try {
      String stmtText =
          "UPDATE " + MODIFY_TABLE +
          " set id = 2 + 3 + smallint_col" +
          " where 1 + 1 + id = 8 + 2";
        analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      verifyErrorMsg(e, "Key column cannot be updated");
      // Message does not include the rhs
    }
  }

  @Test
  public void testDuplicateUpdate() {
    try {
      String stmtText =
          "UPDATE " + MODIFY_TABLE +
          " set int_col = 2 + 3 + smallint_col," +
          " int_col = 10" +
          " where 1 + 1 + id = 8 + 2";
        analyzeSelect(stmtText);
      fail();
    } catch (AnalysisException e) {
      verifyErrorMsg(e, "Duplicate value assignment");
    }
  }

  // TODO: Assignment type conflict.

  //-----------------------------------------------------------------
  // INSERT statement

  public static final String INSERT_TABLE = "functional.alltypestiny";

  private InsertStmt analyzeInsert(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return (InsertStmt) query.parseNode();
  }

  /**
   * Sanity test of INSERT ... PARITION ... VALUES ... rewrite
   */
  @Test
  public void testInsert() throws AnalysisException {
    String stmtText =
        "insert into " + INSERT_TABLE +
        " (id, int_col)" +
        " partition (month = 2 + 3, year = 2000 + 18)" +
        " values (1 + 1, 8 + 2)";
    InsertStmt stmt = analyzeInsert(stmtText);
    ValuesStmt valuesStmt = (ValuesStmt) stmt.getQueryStmt();

    // Verify values are rewritten and analyzed
    List<List<Expr>> values = valuesStmt.getValues();
    Expr value1 = values.get(0).get(0);
    Expr value2 = values.get(0).get(1);

    assertTrue(value1.isAnalyzed());
    assertEquals(ScalarType.SMALLINT, value1.getType());
    assertEquals(1.0, value1.getCost(), 0.1);
    assertEquals("2", value1.toSql());

    assertTrue(value2.isAnalyzed());
    assertEquals(ScalarType.SMALLINT, value2.getType());
    assertEquals(1.0, value2.getCost(), 0.1);
    assertEquals("10", value2.toSql());

    List<PartitionKeyValue> keys = stmt.getPartitionKeyValues();
    PartitionKeyValue key1 = keys.get(0);
    PartitionKeyValue key2 = keys.get(1);

    assertTrue(key1.getValue().isAnalyzed());
    assertEquals(ScalarType.SMALLINT, key1.getValue().getType());
    assertEquals(3.0, key1.getValue().getCost(), 0.1);
    assertEquals("5", key1.getLiteralValue().toSql());

    assertTrue(key2.getValue().isAnalyzed());
    assertEquals(ScalarType.INT, key2.getValue().getType());
    assertEquals(3.0, key2.getValue().getCost(), 0.1);
    assertEquals("2018", key2.getLiteralValue().toSql());

    // Statement's toSql should be before rewrites
    String origSql =
        "INSERT INTO TABLE " + INSERT_TABLE +
        " (id, int_col)" +
        " PARTITION (month = 2 + 3, year = 2000 + 18)" +
        " VALUES (1 + 1, 8 + 2)";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "INSERT INTO TABLE " + INSERT_TABLE +
        " (id, int_col)" +
        " PARTITION (month = 5, year = 2018)" +
        " VALUES (2, 10)";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  @Test
  public void testInsertWith() throws AnalysisException {
    String stmtText =
        "with" +
        " query1 (a, b) as" +
        " (select cast(1 + 1 + id as int)," +
        " cast(2 + 3 + int_col as int)" +
        " from " + SELECT_TABLE + ")" +
        " insert into " + INSERT_TABLE +
        " (id, int_col)" +
        " partition (month = 5, year = 2018)" +
        " select * from query1";
    InsertStmt stmt = analyzeInsert(stmtText);
    WithClause withClause = stmt.getWithClause();
    View view = withClause.getViews().get(0);

    // Sanity check of column labels

    List<String> labels = view.getColLabels();
    assertEquals(2, labels.size());
    assertEquals("a", labels.get(0));
    assertEquals("b", labels.get(1));

    // Verify view was rewritten

    SelectStmt selectStmt = (SelectStmt) view.getQueryStmt();
    List<SelectListItem> selectList = selectStmt.getSelectList().getItems();

    Expr expr1 = selectList.get(0).getExpr();
    Expr expr2 = selectList.get(1).getExpr();

    // Should have been re-analyzed after rewrite
    assertTrue(expr1.isAnalyzed());
    assertEquals(ScalarType.INT, expr1.getType());
    assertEquals(8.0, expr1.getCost(), 0.1);
    assertEquals("CAST(2 + id AS INT)", expr1.toSql());

    assertTrue(expr2.isAnalyzed());
    assertEquals(ScalarType.INT, expr2.getType());
    assertEquals(8.0, expr2.getCost(), 0.1);
    assertEquals("CAST(5 + int_col AS INT)", expr2.toSql());

    // Statement's toSql should be before rewrites
    String origSql =
        "WITH" +
        " query1 (a, b) AS" +
        " (SELECT CAST(1 + 1 + id AS INT)," +
        " CAST(2 + 3 + int_col AS INT)" +
        " FROM " + SELECT_TABLE + ")" +
        " INSERT INTO TABLE " + INSERT_TABLE +
        " (id, int_col)" +
        " PARTITION (month = 5, year = 2018)" +
        " SELECT * FROM query1";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "WITH" +
        " query1 (a, b) AS" +
        " (SELECT CAST(2 + id AS INT)," +
        " CAST(5 + int_col AS INT)" +
        " FROM " + SELECT_TABLE + ")" +
        " INSERT INTO TABLE " + INSERT_TABLE +
        " (id, int_col)" +
        " PARTITION (month = 5, year = 2018)" +
        " SELECT * FROM query1";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }

  //-----------------------------------------------------------------
  // VALUES statement

  private ValuesStmt analyzeValues(String stmtText) throws AnalysisException {
    QueryFixture query = fixture.query(stmtText);
    query.analyze();
    return (ValuesStmt) query.parseNode();
  }

  /**
   * Sanity test of SELECT ... FROM VALUES rewrite
   */
  @Test
  public void testValues() throws AnalysisException {
    String stmtText =
        "values (1 + 1 a, 2 * 10 b)";
    ValuesStmt stmt = analyzeValues(stmtText);

    // Verify values are rewritten and analyzed
    List<List<Expr>> values = stmt.getValues();
    Expr value1 = values.get(0).get(0);
    Expr value2 = values.get(0).get(1);

    assertTrue(value1.isAnalyzed());
    assertEquals(ScalarType.SMALLINT, value1.getType());
    assertEquals(1.0, value1.getCost(), 0.1);
    assertEquals("2", value1.toSql());

    assertTrue(value2.isAnalyzed());
    assertEquals(ScalarType.SMALLINT, value2.getType());
    assertEquals(1.0, value2.getCost(), 0.1);
    assertEquals("20", value2.toSql());

    // Statement's toSql should be before rewrites
    String origSql =
        "VALUES (1 + 1 AS a, 2 * 10 AS b)";
    assertEquals(origSql, stmt.toSql());
    assertEquals(origSql, stmt.toSql(false));

    // Rewritten should be available when requested
    String rewrittenSql =
        "VALUES (2 AS a, 20 AS b)";
    assertEquals(rewrittenSql, stmt.toSql(true));
  }
}
