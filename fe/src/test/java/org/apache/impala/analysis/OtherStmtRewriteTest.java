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
 * Test of analyzer internals around expression rewrites
 * in statements other than SELECT. See
 * {@link SelectRewriteTest} for additional details.
 */
public class OtherStmtRewriteTest extends FrontendTestBase {

  public static final String SELECT_TABLE = "functional.alltypestiny";

  // Delete is only defined for Kudu tables
  public static final String MODIFY_TABLE = "functional_kudu.alltypestiny";

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  //-----------------------------------------------------------------
  // DELETE statement
  //
  // Delete is only defined for Kudu tables

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
      analyzeUpdate(stmtText);
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
      analyzeUpdate(stmtText);
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
      analyzeUpdate(stmtText);
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

  /**
   * TEST WITH ... INSERT ...
   */
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
