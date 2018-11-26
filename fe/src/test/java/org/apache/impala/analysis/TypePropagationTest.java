package org.apache.impala.analysis;

import static org.junit.Assert.*;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.treevis.AstPrinter;
import org.junit.Test;

public class TypePropagationTest extends FrontendTestBase {

  /**
   * Analyze a statement with or without rewrites enabled.
   */
  private ParseNode analyze(String query, boolean rewrite) {
    return AnalyzesOk(query, makeContext(rewrite));
  }

  private AnalysisContext makeContext(boolean rewrite) {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setEnable_expr_rewrites(rewrite);
    return ctx;
  }

  /**
   * Analyze a SELECT statement with our without rewrites enabled.
   */
  private SelectStmt analyzeSelect(String query, boolean rewrite) {
    return (SelectStmt) analyze(query, rewrite);
  }

  /**
   * Get the first expression from the SELECT list
   */
  private static Expr firstExpr(SelectStmt stmt) {
    return stmt.getSelectList().getItems().get(0).getExpr();
  }

  /**
   * Get the first expression from the base table result list.
   */
  private static Expr firstBaseTblResult(QueryStmt stmt) {
    return stmt.getBaseTblResultExprs().get(0);
  }

  /**
   * Get the first result expression.
   */
  public Expr firstResult(QueryStmt stmt) {
    return stmt.getResultExprs().get(0);
  }

  /**
   * Get the first output column definition for the
   * CTAS AS SELECT statement
   */
  private static ColumnDef firstColumn(CreateTableAsSelectStmt stmt) {
    return stmt.getCreateStmt().getColumnDefs().get(0);
  }

  /**
   * Generate a simple SELECT with the given expression
   */
  private static String makeSelect(String exprText) {
    return "SELECT " + exprText + " AS c" +
        " from functional.alltypestiny";
  }

  /**
   * Create a SELECT for the expression, analyze it, and return
   * the SELECT list expression node that corresponds to the input
   * expression.
   */
  private Expr analyzeToExpr(String exprText, boolean rewrite) {
    String query = makeSelect(exprText);
    return firstExpr(analyzeSelect(query, rewrite));
  }

  private void expectExprError(String exprText, boolean rewrite) throws AnalysisException {
    String query = makeSelect(exprText);
    try {
      parseAndAnalyze(query, makeContext(rewrite));
    } catch (AnalysisException e) {
      throw e;
    } catch (ImpalaException e) {
      fail(e.getMessage());
    }
    fail();
  }

  /**
   * Create a values SELECT for the expression, analyze it, and return
   * the SELECT list expression node that corresponds to the input
   * expression.
   */
  private Expr analyzeValuesExpr(String exprText, boolean rewrite) {
    String query = "SELECT " + exprText;
    return firstExpr(analyzeSelect(query, rewrite));
  }

  private static boolean isExplicitCast(Expr expr) {
    if (expr instanceof CastExpr) {
      return !((CastExpr) expr).isImplicit();
    } else if (expr instanceof NumericLiteral) {
      return ((NumericLiteral) expr).isExplicitCast();
    } // Want to test NULL literal, but no info
    fail();
    return false; // To keep compiler happy
  }

  private static int intValue(Expr expr) {
    if (expr instanceof CastExpr)
      return intValue(expr.getChild(0));
    return (int) ((NumericLiteral) expr).getIntValue();
  }

  private static double doubleValue(Expr expr) {
    if (expr instanceof CastExpr)
      return doubleValue(expr.getChild(0));
    return ((NumericLiteral) expr).getDoubleValue();
  }

  private void testValues(boolean rewritten) {
    {
      Expr expr = analyzeValuesExpr("1", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
    }
    {
      Expr expr = analyzeValuesExpr("1 + 1", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
    }
    {
      Expr expr = analyzeValuesExpr("1000", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
    }
  }

  /**
   * Test type propagation in a simple values SELECT
   * (one without a FROM clause).
   *
   * Just verifies that behavior is the same as a normal
   * SELECT, does not do extensive probing.
   */
  @Test
  public void testValues() {
    testValues(false);
    testValues(true);
  }

  private void doTestBasics(boolean rewritten) {
    {
      Expr expr = analyzeToExpr("1", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
    }
    {
      Expr expr = analyzeToExpr("1000", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
    }
    {
      Expr expr = analyzeToExpr("CAST(1 AS INT)", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      // Implicit casts for numeric literals is part of
      // rewrites. Else, the concrete cast remains.
      // Bug: Explict cast not retained on rewrite
      if (! rewritten) { // Remove this if when fixed
        assertTrue(isExplicitCast(expr));
      }
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
      } else {
        assertTrue(expr instanceof CastExpr);
      }
    }
    {
      Expr expr = analyzeToExpr("NULL", rewritten);
      assertEquals(ScalarType.NULL, expr.getType());
    }
    {
      Expr expr = analyzeToExpr("CAST(NULL AS SMALLINT)", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        // Bug: Cast should be pushed into NULL as shown
        // later for constant expressions
        //assertTrue(expr instanceof NullLiteral);
      } else {
        assertTrue(expr instanceof CastExpr);
        assertTrue(isExplicitCast(expr));
      }
    }
  }

  /**
   * Tests basic SELECT behavior with numeric and null literals.
   */
  @Test
  public void testBasics() {
    doTestBasics(false);
    doTestBasics(true);
  }

  @SuppressWarnings("unused")
  private void testTypeWidening(boolean rewritten) {
    // Sum of two TINYINT, widened to SMALLINT
    {
      Expr expr = analyzeToExpr("1 + 1", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(2, intValue(expr));
       } else {
        assertTrue(expr instanceof ArithmeticExpr);

        Expr left = expr.getChild(0);
        assertEquals(ScalarType.SMALLINT, left.getType());
        assertTrue(left instanceof NumericLiteral);
        assertFalse(isExplicitCast(left));

        Expr right = expr.getChild(1);
        assertEquals(ScalarType.SMALLINT, right.getType());
        assertTrue(right instanceof NumericLiteral);
        assertFalse(isExplicitCast(right));
      }
    }
    // Explicit types for operands of addition
    {
      Expr expr = analyzeToExpr("CAST(1 AS TINYINT) + CAST(1 AS TINYINT)", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        // TODO: This is ambiguous.
        assertFalse(isExplicitCast(expr));
        assertEquals(2, intValue(expr));
      } else {
        assertTrue(expr instanceof ArithmeticExpr);

        // TODO: Expected the two casts to be pushed into the
        // numeric constant as was done in other cases.
        Expr left = expr.getChild(0);
        assertEquals(ScalarType.SMALLINT, left.getType());
        assertTrue(left instanceof CastExpr);
        assertFalse(isExplicitCast(left));

        Expr child = left.getChild(0);
        assertTrue(child instanceof CastExpr);
        assertTrue(isExplicitCast(child));
        assertEquals(ScalarType.TINYINT, child.getType());

        Expr right = expr.getChild(1);
        assertEquals(ScalarType.SMALLINT, right.getType());
        assertTrue(right instanceof CastExpr);
        assertFalse(isExplicitCast(right));

        child = left.getChild(0);
        assertTrue(child instanceof CastExpr);
        assertTrue(isExplicitCast(child));
        assertEquals(ScalarType.TINYINT, child.getType());
      }
    }
    // Excessive type widening
    {
      Expr expr = analyzeToExpr("1 + 1 + 1", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(3, intValue(expr));
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    // Implicit type of SMALLINT
    {
      Expr expr = analyzeToExpr("1000 + 1000", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(2000, intValue(expr));
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    // Entire sum is cast to a specific type
    {
      Expr expr = analyzeToExpr("CAST(1 + 1 AS TINYINT)", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
      if (rewritten) {
        assertFalse(isExplicitCast(expr));
      } else {
        assertTrue(expr instanceof CastExpr);
        assertTrue(isExplicitCast(expr));
      }
    }
  }

  @Test
  public void testTypeWidening() {
    testTypeWidening(false);
    testTypeWidening(true);
  }

  private void testTypeWideningWithNull(boolean rewritten) {
    {
      // (+ 1:SMALLINT NULL:SMALLINT)
      Expr expr = analyzeToExpr("1 + NULL", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        assertEquals(ScalarType.SMALLINT, expr.getType());
        assertTrue(expr instanceof NullLiteral);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
        Expr left = expr.getChild(0);
        assertEquals(ScalarType.SMALLINT, left.getType());
        assertTrue(left instanceof NumericLiteral);
        Expr right = expr.getChild(1);
        assertEquals(ScalarType.SMALLINT, right.getType());
        assertTrue(right instanceof NullLiteral);
      }
    }
    {
      Expr expr = analyzeToExpr("1 + 1 + NULL", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NullLiteral);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    {
      Expr expr = analyzeToExpr("1 + CAST(NULL AS TINYINT)", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        // Bug, is actually an ArithmeticExpr
        //assertTrue(expr instanceof NullLiteral);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
        Expr left = expr.getChild(0);
        assertEquals(ScalarType.SMALLINT, left.getType());
        assertTrue(left instanceof NumericLiteral);
        assertFalse(isExplicitCast(left));

        Expr right = expr.getChild(1);
        assertEquals(ScalarType.SMALLINT, right.getType());
        assertTrue(right instanceof CastExpr);

        // TODO: CASTs not combined and pushed into Null literal
        Expr child = right.getChild(0);
        assertTrue(child instanceof CastExpr);
        assertEquals(ScalarType.TINYINT, child.getType());
        assertTrue(isExplicitCast(child));

        Expr leaf = child.getChild(0);
        assertTrue(leaf instanceof NullLiteral);
        // Actual type is TINYINT, which is odd given that the
        // cast has not been pushed down...
        //assertEquals(ScalarType.NULL, leaf.getType());
      }
    }
  }

  @Test
  public void testTypeWideningWithNull() {
    testTypeWideningWithNull(false);
    testTypeWideningWithNull(true);
  }

  private void testCasts(boolean rewritten) {
    {
      Expr expr = analyzeToExpr("1", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
      assertTrue(expr instanceof NumericLiteral);
    }
    {
      Expr expr = analyzeToExpr("CAST(1 AS SMALLINT)", rewritten);
      assertEquals(ScalarType.SMALLINT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        // Bug: should preserve explicit cast nature
        //assertTrue(((NumericLiteral) expr).isExplicitCast());
      } else {
        // Should rewrite to push cast into literal?
        assertTrue(expr instanceof CastExpr);
        assertTrue(isExplicitCast(expr));
      }
    }
    {
      Expr expr = analyzeToExpr("CAST(1 AS SMALLINT) + 2", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
        Expr left = expr.getChild(0);
        assertEquals(ScalarType.INT, left.getType());
        assertTrue(left instanceof CastExpr);
        assertFalse(isExplicitCast(left));

        Expr leftInner = left.getChild(0);
        assertEquals(ScalarType.SMALLINT, leftInner.getType());
        assertTrue(leftInner instanceof CastExpr);
        assertTrue(isExplicitCast(leftInner));

        Expr leftLeaf = leftInner.getChild(0);
        assertEquals(ScalarType.TINYINT, leftLeaf.getType());
        assertTrue(leftLeaf instanceof NumericLiteral);

        Expr right = expr.getChild(1);
        assertEquals(ScalarType.INT, right.getType());
        assertTrue(right instanceof NumericLiteral);
      }
    }
    // Test round down
    {
      Expr expr = analyzeToExpr("CAST(1.23 AS TINYINT)", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
      if (rewritten) {
        assertEquals(1.0D, doubleValue(expr), 0.00001);
      } else {
        assertTrue(expr instanceof CastExpr);
        assertTrue(isExplicitCast(expr));

        Expr leaf = expr.getChild(0);
        assertEquals(ScalarType.createDecimalType(3, 2), leaf.getType());
        assertTrue(leaf instanceof NumericLiteral);
        assertEquals(1.23D, doubleValue(leaf), 0.00001);
      }
    }
    // Test round up
    {
      Expr expr = analyzeToExpr("CAST(1.78 AS TINYINT)", rewritten);
      assertEquals(ScalarType.TINYINT, expr.getType());
      if (rewritten) {
        assertEquals(2.0D, doubleValue(expr), 0.00001);
      } else {
        assertTrue(expr instanceof CastExpr);
        assertTrue(isExplicitCast(expr));

        Expr leaf = expr.getChild(0);
        assertEquals(ScalarType.createDecimalType(3, 2), leaf.getType());
        assertTrue(leaf instanceof NumericLiteral);
        assertEquals(1.78D, doubleValue(leaf), 0.00001);
      }
    }
    // Test overflow on cast
    {
      String exprStr = "CAST(257 AS TINYINT)";
      if (rewritten) {
        // Analyzer does not catch such errors
        //try {
        //  expectExprError(exprStr, true);
        //  //fail();
        //} catch (AnalysisException e) {
        //  // Do something
        //}
      }
      // Cast would overflow, so is not rewritten
      Expr expr = analyzeToExpr(exprStr, false);
      assertEquals(ScalarType.TINYINT, expr.getType());
      assertTrue(expr instanceof CastExpr);
      assertTrue(isExplicitCast(expr));

      Expr leaf = expr.getChild(0);
      assertEquals(ScalarType.SMALLINT, leaf.getType());
      assertTrue(leaf instanceof NumericLiteral);
      assertEquals(257, intValue(leaf));
    }
    {
      Expr expr = analyzeToExpr("1 + CAST(257 AS TINYINT)", rewritten);
      if (rewritten) {
        // This should fail, should, or should keep the parse
        // tree as above, but actually succeeds and produces the
        // "wrong" result ("right" if we accept that the user
        // intended the value to wrap.)
        // Very confusing.
        assertEquals(2, intValue(expr));
      }
    }
    // While integer values silently overflow, the code does catch
    // Double overflows.
    //
    // See Float.MAX_VALUE and Double.MAX_VALUE for the values used below.
    {
      try {
        expectExprError("2e+308", rewritten);
        fail();
      } catch (AnalysisException e) {
        assertTrue(e.getMessage().contains("exceeds maximum range"));
      }
    }
    // Analyzer postpones the cast until runtime, even when
    // rewritten. Will fail at runtime.
    {
      Expr expr = analyzeToExpr("CAST(1e+300 AS FLOAT)", rewritten);
      assertEquals(ScalarType.FLOAT, expr.getType());
      assertTrue(expr instanceof CastExpr);
    }
    // Expression is rewritten because the value fits
    {
      Expr expr = analyzeToExpr("1 + 1e+300", rewritten);
      assertEquals(ScalarType.DOUBLE, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(1e300, doubleValue(expr), 1e290);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    // Expression is rewritten because the value fits
    {
      Expr expr = analyzeToExpr("1 + CAST(1e+30 AS FLOAT)", rewritten);
      assertEquals(ScalarType.DOUBLE, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(1e30, doubleValue(expr), 1e27);
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    // Expression is not rewritten because of overflow.
    // Query will fail (maybe?) at run time.
    {
      Expr expr = analyzeToExpr("1 + CAST(1e+300 AS FLOAT)", rewritten);
      assertEquals(ScalarType.DOUBLE, expr.getType());
      assertTrue(expr instanceof ArithmeticExpr);
      assertTrue(isExplicitCast(expr.getChild(1).getChild(0)));
    }
    // Valid cast from string
    {
      Expr expr = analyzeToExpr("1 + CAST('123' AS SMALLINT)", rewritten);
      assertEquals(ScalarType.INT, expr.getType());
      if (rewritten) {
        assertTrue(expr instanceof NumericLiteral);
        assertEquals(124, intValue(expr));
      } else {
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
    // Invalid cast from string
    {
      // Expected failure on rewriting, but instead
      // creates an invalid expression
      Expr expr = analyzeToExpr("1 + CAST('ABC' AS SMALLINT)", rewritten);
      // Bug: Fails when rewritten: SMALLINT instead of INT
      if (! rewritten) {
        assertEquals(ScalarType.INT, expr.getType());
      }
      assertTrue(expr instanceof ArithmeticExpr);
    }
    try {
      expectExprError("CAST('true' AS BOOLEAN)", rewritten);
    } catch (AnalysisException e) {
      assertTrue(e.getMessage().contains("Invalid type cast"));
    }
  }

  @Test
  public void testCasts() {
    testCasts(false);
    testCasts(true);
  }

  private void testNestedSelect(boolean rewritten) {
    String query  =
        "create table ctas_test as select 1 + 1";
    CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) analyze(query, rewritten);
    SelectStmt select = (SelectStmt) stmt.getQueryStmt();
    // Fails, returns TINYINT when rewritten = true
    if (! rewritten) {
    assertEquals(ScalarType.SMALLINT, firstExpr(select).getType());
    assertEquals(ScalarType.SMALLINT, firstResult(select).getType());
    }

    InsertStmt insertStmt = stmt.getInsertStmt();
    select = (SelectStmt) insertStmt.getQueryStmt();
    // Fails, returns TINYINT with rewritten = true
    if (! rewritten) {
    assertEquals(ScalarType.SMALLINT, firstExpr(select).getType());
    assertEquals(ScalarType.SMALLINT, firstResult(select).getType());
    }

    assertEquals(ScalarType.SMALLINT, firstColumn(stmt).getType());
  }

  @Test
  public void testNestedSelect() {
    testNestedSelect(false);
    testNestedSelect(true);
  }

  private void testResult(String exprText, boolean rewritten) {
    SelectStmt stmt = analyzeSelect(makeSelect(exprText), rewritten);
    assertEquals(firstExpr(stmt).getType(), firstResult(stmt).getType());
    assertEquals(firstExpr(stmt).getType(), firstBaseTblResult(stmt).getType());
  }

  private void testResult(boolean rewritten) {
    testResult("1", rewritten);
    // Bug: Fails after rewrites: TINYINT instead of SMALLINT
    if (!rewritten) {
      testResult("1 + 1", rewritten);
    }
    // Bug: Fails after rewrites: TINYINT instead of INT
    if (!rewritten) {
      testResult("CAST(1 AS INT)", rewritten);
    }
  }

  @Test
  public void testResultTuple() {
    testResult(false);
    testResult(true);
  }

  @Test
  public void testTBD1() {
    String query = "SELECT 1 + 1";
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setEnable_expr_rewrites(true);
    SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
    Expr expr = select.getSelectList().getItems().get(0).getExpr();
    // Fails, actual result is TINYINT
    //assertEquals(ScalarType.SMALLINT, expr.getType());
  }

  @Test
  public void testTBD2() {
    {
      String query  =
          "create table ctas_test as select 1 + 1";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) AnalyzesOk(query, ctx);
      SelectStmt select = (SelectStmt) stmt.getQueryStmt();
      Expr expr = select.getSelectList().getItems().get(0).getExpr();
      // Fails, actual result is TINYINT
      //assertEquals(ScalarType.SMALLINT, expr.getType());
      ColumnDef col = stmt.getCreateStmt().getColumnDefs().get(0);
      assertEquals(ScalarType.SMALLINT, col.getType());
    }
  }

  @Test
  public void testNestedTBD3() {
    {
      String query = "SELECT 1 + 1 AS c" +
          " from functional.alltypestiny";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
      Expr selectExpr = select.getSelectList().getItems().get(0).getExpr();
      assertEquals(ScalarType.SMALLINT, selectExpr.getType());
      Expr resultExpr = select.getResultExprs().get(0);
      assertEquals(ScalarType.SMALLINT, resultExpr.getType());
      Expr baseTableExpr = select.getBaseTblResultExprs().get(0);
      // Fails, actual type is TINYINT
      //assertEquals(ScalarType.SMALLINT, baseTableExpr.getType());
    }
    {
      String query = "SELECT CAST(NULL AS SMALLINT) AS c" +
          " from functional.alltypestiny";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
      Expr expr = select.getSelectList().getItems().get(0).getExpr();
      assertEquals(ScalarType.SMALLINT, expr.getType());
      // Fails, is actually CastExpr
      //assertTrue(expr instanceof NullLiteral);
    }
  }

  @Test
  public void testNestedTBD4() {
    {
      String query = "SELECT 1 + NULL AS c" +
          " from functional.alltypestiny";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
      Expr expr = select.getSelectList().getItems().get(0).getExpr();
      assertEquals(ScalarType.SMALLINT, expr.getType());
      assertTrue(expr instanceof NullLiteral);
    }
  }

  @Test
  public void testNestedTBD5() {
    {
      String query = "SELECT CAST(1 AS INT) AS c" +
          " from functional.alltypestiny";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
      Expr expr = select.getSelectList().getItems().get(0).getExpr();
      assertEquals(ScalarType.INT, expr.getType());
      assertTrue(expr instanceof NumericLiteral);
      // Bug: this is an explicit cast
      //assertTrue(((NumericLiteral) expr).isExplicitCast());
    }
  }

  @Test
  public void testTBD6() {
    {
      String query = "SELECT CAST(257 AS TINYINT) AS c" +
          " from functional.alltypestiny";
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
      // Should have failed, instead we get a
      // NumericLiteral of type TININT with value 1
    }
  }

//  /**
//   * Test of a simple addition of two numbers
//   */
//  @Test
//  public void testNestedTBD6() {
//    String query = "SELECT CAST(1 AS SMALLINT) + 2 AS c" +
//        " from functional.alltypestiny";
//    AnalysisContext ctx = createAnalysisCtx();
//    ctx.getQueryOptions().setEnable_expr_rewrites(false);
//    SelectStmt select = (SelectStmt) AnalyzesOk(query, ctx);
//
//    Expr expr = select.getSelectList().getItems().get(0).getExpr();
//    // Fails, is of type INT
//    //assertEquals(ScalarType.SMALLINT, expr.getType());
//    assertTrue(expr instanceof ArithmeticExpr);
//
//    Expr outerLeftCast = expr.getChild(0);
//    // Fails, is of type INT
//    //assertEquals(ScalarType.SMALLINT, left.getType());
//    assertTrue(outerLeftCast instanceof CastExpr);
//    assertTrue(((CastExpr) outerLeftCast).isImplicit());
//
//    Expr innerLeftCast = outerLeftCast.getChild(0);
//    assertEquals(ScalarType.SMALLINT, innerLeftCast.getType());
//    assertTrue(innerLeftCast instanceof CastExpr);
//    // Fails: cast not marked as explicit (non-implicit)
//    //assertFalse(((CastExpr) outerLeftCast).isImplicit());
//
//    Expr leftLeaf = innerLeftCast.getChild(0);
//    assertEquals(ScalarType.TINYINT, leftLeaf.getType());
//    assertTrue(leftLeaf instanceof NumericLiteral);
//
//    Expr right = expr.getChild(1);
//    // Fails, is of type INT
//    //assertEquals(ScalarType.SMALLINT, right.getType());
//    assertTrue(right instanceof NumericLiteral);
//  }

}
