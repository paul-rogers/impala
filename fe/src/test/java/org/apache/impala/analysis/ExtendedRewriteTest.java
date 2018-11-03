package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.apache.impala.analysis.AnalysisFixture.RewriteFixture;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

/**
 * In-depth testing of expression rewrites for cases that cannot easily
 * be probed at the query level.
 */
public class ExtendedRewriteTest extends FrontendTestBase {

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  /**
   * Test the constant folding rule in greater detail than is possible
   * at the level of the query itself.
   */
  @Test
  public void constantFoldTest() throws AnalysisException {
    ExprRewriteRule rule = FoldConstantsRule.INSTANCE;
    RewriteFixture rf = fixture.rewriteFixture();
    rf.select("id");
    Analyzer analyzer = rf.query().analyzer();

    {
      // Non-constant is returned
      Expr expr = rf.selectExpr();
      Expr result = rule.apply(expr, analyzer);
      assertSame(expr, result);
    }

    {
      // Boolean literal is returned
      Expr expr = new BoolLiteral(true);
      expr.analyze(analyzer);
      Expr result = rule.apply(expr, analyzer);
      assertSame(expr, result);

      // Cast of Boolean to itself returns value
      Expr expr2 = new CastExpr(Type.BOOLEAN, expr);
      expr2.analyze(analyzer);
      result = rule.apply(expr2, analyzer);
      assertTrue(Expr.IS_TRUE_LITERAL.apply(result));
    }

    {
      // Null is returned
      Expr expr = new NullLiteral();
      expr.analyze(analyzer);
      Expr result = rule.apply(expr, analyzer);
      assertSame(expr, result);

      // Cast of NULL to any type is returned
      Expr expr2 = new CastExpr(Type.BOOLEAN, expr);
      expr2.analyze(analyzer);
      result = rule.apply(expr2, analyzer);
      assertSame(expr2, result);

      expr2 = new CastExpr(Type.DECIMAL, expr);
      expr2.analyze(analyzer);
      result = rule.apply(expr2, analyzer);
      assertSame(expr2, result);
    }

    {
      // Non-null literal is returned
      Expr expr = new NumericLiteral(new BigDecimal(1), Type.TINYINT);
      expr.analyze(analyzer);
      Expr result = rule.apply(expr, analyzer);
      assertSame(expr, result);

      // Non-null cast is evaluated
      Expr expr2 = new CastExpr(Type.BIGINT, expr);
      expr2.analyze(analyzer);
      result = rule.apply(expr2, analyzer);
      assertTrue(result instanceof NumericLiteral);
      assertEquals(1L, ((NumericLiteral) result).getIntValue());
    }

    {
      // Decimal literal is returned
      Expr expr = new NumericLiteral(new BigDecimal(1), Type.TINYINT);
      expr.analyze(analyzer);
      Expr result = rule.apply(expr, analyzer);
      assertSame(expr, result);

      // But, for decimal overflow, the original expression
      // is returned, marked as non-const.
      Expr expr2 = new CastExpr(ScalarType.createDecimalType(38,38), expr);
      expr2.analyze(analyzer);
      result = rule.apply(expr2, analyzer);
      assertSame(expr2, result);
      assertFalse(expr2.isConstant());
    }

    {
      // If no cast needed, no cast is added
      Expr expr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
          new NumericLiteral(new BigDecimal(1), Type.TINYINT),
          new NumericLiteral(new BigDecimal(2), Type.TINYINT));
      expr.analyze(analyzer);
      assertEquals(Type.SMALLINT, expr.getType());
      Expr result = rule.apply(expr, analyzer);
      assertTrue(result instanceof NumericLiteral);
      assertEquals(3L, ((NumericLiteral) result).getIntValue());
      assertEquals(Type.SMALLINT, result.getType());
    }

    {
      // Cast needed
      Expr expr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
          new NumericLiteral(new BigDecimal(1), Type.TINYINT),
          new NumericLiteral(new BigDecimal(2), Type.TINYINT));
      expr.analyze(analyzer);
      expr.type_ = Type.BIGINT;
      Expr result = rule.apply(expr, analyzer);
      assertTrue(result instanceof NumericLiteral);
      assertEquals(Type.BIGINT, result.getType());
      assertEquals(3L, ((NumericLiteral) result).getIntValue());
    }
  }

  @Test
  public void testDecimalCast() throws ImpalaException {
    String testExpr = "coalesce(1.8, cast(0 as decimal(38,38)))";

    // Decimal V1, a decimal overflow produces NULL which coalesce()
    // can catch. DO NOT rewrite the expression to "1.8".
    TQueryOptions options = fixture.cloneOptions();
    options.setDecimal_v2(false);
    RewriteFixture rf = fixture.rewriteFixture(options).select(testExpr);
    // Note: toSql() hides casts
    rf.verifySelect("coalesce(1.8, 0)");
    Expr result = rf.selectExpr();
    assertEquals(ScalarType.createDecimalType(38, 38), result.getType());

    // Decimal V2 raises an error about casts, so handling of the
    // casted 1.8 is moot.
    try {
      rf = fixture.rewriteFixture().select(testExpr);
    } catch (AssertionError e) {
      assertTrue(e.getMessage().contains("Cannot resolve DECIMAL types of the " +
            "coalesce(DECIMAL(2,1), DECIMAL(38,38)) function arguments."));
    }
  }
}
