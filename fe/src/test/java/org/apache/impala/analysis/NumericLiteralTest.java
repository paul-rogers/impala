package org.apache.impala.analysis;

import static org.junit.Assert.*;

import java.math.BigDecimal;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.common.SqlCastException;
import org.junit.Test;

public class NumericLiteralTest {

  // Approximate maximum DECIMAL scale for a BIGINT
  // 9,223,372,036,854,775,807
  private static final int MAX_BIGINT_PRECISION = 19;

  private static final BigDecimal ABOVE_TINYINT =
      NumericLiteral.MAX_TINYINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_TINYINT =
      NumericLiteral.MIN_TINYINT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_SMALLINT =
      NumericLiteral.MAX_SMALLINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_SMALLINT =
      NumericLiteral.MIN_SMALLINT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_INT =
      NumericLiteral.MAX_INT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_INT =
      NumericLiteral.MIN_INT.subtract(BigDecimal.ONE);
  private static final BigDecimal ABOVE_BIGINT =
      NumericLiteral.MAX_BIGINT.add(BigDecimal.ONE);
  private static final BigDecimal BELOW_BIGINT =
      NumericLiteral.MIN_BIGINT.subtract(BigDecimal.ONE);

  private static String repeat(String str, int n) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < n; i++) buf.append(str);
    return buf.toString();
  }

  private static String genDecimal(int precision, int scale) {
    if (scale == 0) {
      return repeat("9", precision);
    } else {
      return repeat("9", precision - scale) + "." +
             repeat("4", scale);
    }
  }

  @Test
  public void testConstants() throws InvalidValueException {
    assertEquals(BigDecimal.valueOf(Byte.MIN_VALUE), NumericLiteral.MIN_TINYINT);
    assertEquals(BigDecimal.valueOf(Byte.MAX_VALUE), NumericLiteral.MAX_TINYINT);
    assertEquals(BigDecimal.valueOf(Short.MIN_VALUE), NumericLiteral.MIN_SMALLINT);
    assertEquals(BigDecimal.valueOf(Short.MAX_VALUE), NumericLiteral.MAX_SMALLINT);
    assertEquals(BigDecimal.valueOf(Integer.MIN_VALUE), NumericLiteral.MIN_INT);
    assertEquals(BigDecimal.valueOf(Integer.MAX_VALUE), NumericLiteral.MAX_INT);
    assertEquals(BigDecimal.valueOf(Long.MIN_VALUE), NumericLiteral.MIN_BIGINT);
    assertEquals(BigDecimal.valueOf(Long.MAX_VALUE), NumericLiteral.MAX_BIGINT);
    assertEquals(BigDecimal.valueOf(-Float.MAX_VALUE), NumericLiteral.MIN_FLOAT);
    assertEquals(BigDecimal.valueOf(Float.MAX_VALUE), NumericLiteral.MAX_FLOAT);
    assertEquals(BigDecimal.valueOf(-Double.MAX_VALUE), NumericLiteral.MIN_DOUBLE);
    assertEquals(BigDecimal.valueOf(Double.MAX_VALUE), NumericLiteral.MAX_DOUBLE);
  }

  @Test
  public void testInferType() throws InvalidValueException {
    assertEquals(Type.TINYINT, NumericLiteral.inferType(BigDecimal.ZERO));
    assertEquals(Type.TINYINT, NumericLiteral.inferType(NumericLiteral.MIN_TINYINT));
    assertEquals(Type.TINYINT, NumericLiteral.inferType(NumericLiteral.MAX_TINYINT));

    assertEquals(Type.SMALLINT, NumericLiteral.inferType(ABOVE_TINYINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(BELOW_TINYINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(NumericLiteral.MIN_SMALLINT));
    assertEquals(Type.SMALLINT, NumericLiteral.inferType(NumericLiteral.MAX_SMALLINT));

    assertEquals(Type.INT, NumericLiteral.inferType(ABOVE_SMALLINT));
    assertEquals(Type.INT, NumericLiteral.inferType(BELOW_SMALLINT));
    assertEquals(Type.INT, NumericLiteral.inferType(NumericLiteral.MIN_INT));
    assertEquals(Type.INT, NumericLiteral.inferType(NumericLiteral.MAX_INT));

    assertEquals(Type.BIGINT, NumericLiteral.inferType(ABOVE_INT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(BELOW_INT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(NumericLiteral.MIN_BIGINT));
    assertEquals(Type.BIGINT, NumericLiteral.inferType(NumericLiteral.MAX_BIGINT));

    // 9.9. Is DECIMAL
    assertEquals(ScalarType.createDecimalType(2, 1),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(2, 1))));
    assertEquals(ScalarType.createDecimalType(2, 1),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(2, 1)).negate()));

    // One bigger or smaller than BIGINT
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(ABOVE_BIGINT));
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(BELOW_BIGINT));

    // All 9s, just bigger than BIGINT
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(MAX_BIGINT_PRECISION, 0))));
    assertEquals(ScalarType.createDecimalType(MAX_BIGINT_PRECISION, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(MAX_BIGINT_PRECISION, 0)).negate()));

    // All 9s, at limits of DECIMAL precision
    assertEquals(ScalarType.createDecimalType(ScalarType.MAX_SCALE, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE, 0))));
    assertEquals(ScalarType.createDecimalType(ScalarType.MAX_SCALE, 0),
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE, 0)).negate()));

    // Too large for DECIMAL, flips to DOUBLE
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE + 1, 0))));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            new BigDecimal(genDecimal(ScalarType.MAX_SCALE + 1, 0)).negate()));

    // Too large for Decimal, small enough for FLOAT
    // DECIMAL range is e38 as is FLOAT. So, there is a small range
    // in which we flip from DECIMAL to FLOAT.
    assertEquals(Type.FLOAT,
        NumericLiteral.inferType(NumericLiteral.MIN_FLOAT));
    assertEquals(Type.FLOAT,
        NumericLiteral.inferType(NumericLiteral.MAX_FLOAT));
    // Float.MIN_VALUE means smallest positive value, confusingly
    assertEquals(Type.FLOAT,
        NumericLiteral.inferType(new BigDecimal(Float.MIN_VALUE)));
    assertEquals(Type.FLOAT,
        NumericLiteral.inferType(new BigDecimal(-Float.MIN_VALUE)));

    // Too large for Decimal, exponent too large for FLOAT
    String value = "12345" + repeat("0", 40);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    value = repeat("9", 10) + repeat("0", 40);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    // Too many digits for DOUBLE, but exponent fits
    value = repeat("9", 30) + repeat("0", 50);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));
    value = genDecimal(100, 10);
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(value).negate()));

    // Limit of DOUBLE range
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MIN_DOUBLE));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(NumericLiteral.MAX_DOUBLE));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(Double.MIN_VALUE)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(new BigDecimal(-Double.MIN_VALUE)));

    // Too big for DOUBLE or DECIMAL
    try {
      NumericLiteral.inferType(new BigDecimal(genDecimal(309, 0)));
      fail();
    } catch (InvalidValueException e) {
      // Expected
    }

    // Overflows DOUBLE, but low digits are truncated, so is DOUBLE
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            NumericLiteral.MAX_DOUBLE.add(BigDecimal.ONE)));
    assertEquals(Type.DOUBLE,
        NumericLiteral.inferType(
            NumericLiteral.MAX_DOUBLE.add(BigDecimal.ONE).negate()));

    // Another power of 10, actual overflow
    try {
      NumericLiteral.inferType(
          NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch (InvalidValueException e) {
      // Expected
    }
    try {
      NumericLiteral.inferType(
          NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN).negate());
      fail();
    } catch (InvalidValueException e) {
      // Expected
    }

    // Too small for DOUBLE, too many digits for DECIMAL
    // BUG: Should round to zero per SQL standard.
    value = "." + repeat("0", 325) + "1";
    try {
      NumericLiteral.inferType(new BigDecimal(value));
      fail();
    } catch (InvalidValueException e) {
      // Expected
    }
  }

  @Test
  public void testIsOverflow() throws InvalidValueException {
    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.TINYINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_TINYINT, Type.TINYINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_TINYINT, Type.TINYINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_TINYINT, Type.TINYINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_TINYINT, Type.TINYINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.SMALLINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_SMALLINT, Type.SMALLINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_SMALLINT, Type.SMALLINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_SMALLINT, Type.SMALLINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_SMALLINT, Type.SMALLINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.INT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_INT, Type.INT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_INT, Type.INT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_INT, Type.INT));
    assertTrue(NumericLiteral.isOverflow(BELOW_INT, Type.INT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.BIGINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_BIGINT, Type.BIGINT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_BIGINT, Type.BIGINT));
    assertTrue(NumericLiteral.isOverflow(ABOVE_BIGINT, Type.BIGINT));
    assertTrue(NumericLiteral.isOverflow(BELOW_BIGINT, Type.BIGINT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_FLOAT, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_FLOAT, Type.FLOAT));
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(Float.MIN_VALUE), Type.FLOAT));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_FLOAT.add(BigDecimal.ONE), Type.FLOAT));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_FLOAT.multiply(BigDecimal.TEN), Type.FLOAT));
    // Underflow is not overflow
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(
        Float.MIN_VALUE).divide(BigDecimal.TEN), Type.FLOAT));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MIN_DOUBLE, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(NumericLiteral.MAX_DOUBLE, Type.DOUBLE));
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(Double.MIN_VALUE), Type.DOUBLE));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_DOUBLE.add(BigDecimal.ONE), Type.DOUBLE));
    assertTrue(NumericLiteral.isOverflow(
        NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN), Type.DOUBLE));
    // Underflow is not overflow
    assertFalse(NumericLiteral.isOverflow(BigDecimal.valueOf(
        Double.MIN_VALUE).divide(BigDecimal.TEN), Type.DOUBLE));

    assertFalse(NumericLiteral.isOverflow(BigDecimal.ZERO, Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(new BigDecimal(genDecimal(10,5)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(new BigDecimal(genDecimal(10,5)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(new BigDecimal(genDecimal(ScalarType.MAX_PRECISION, 0)), Type.DECIMAL));
    assertFalse(NumericLiteral.isOverflow(new BigDecimal(genDecimal(0, ScalarType.MAX_PRECISION)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(new BigDecimal(genDecimal(ScalarType.MAX_PRECISION + 1, 0)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(new BigDecimal(genDecimal(ScalarType.MAX_PRECISION + 1, 1)), Type.DECIMAL));
    assertTrue(NumericLiteral.isOverflow(new BigDecimal(genDecimal(0, ScalarType.MAX_PRECISION + 1)), Type.DECIMAL));
  }

  @Test
  public void testSimpleCtor() throws InvalidValueException {
    // Spot check. Assumes uses inferType() tested above.
    NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
    assertEquals(0, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_TINYINT);
    assertEquals(Byte.MAX_VALUE, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_BIGINT);
    assertEquals(Type.BIGINT, n.getType());
    assertEquals(Long.MAX_VALUE, n.getLongValue());

    n = new NumericLiteral(NumericLiteral.MAX_DOUBLE);
    assertEquals(Double.MAX_VALUE, n.getDoubleValue(), 1.0);
    assertEquals(Type.DOUBLE, n.getType());

    n = new NumericLiteral(new BigDecimal(genDecimal(35, 0)));
    assertEquals(ScalarType.createDecimalType(35, 0), n.getType());

    try {
      new NumericLiteral(NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch(InvalidValueException e) {
      // Expected
    }
  }

  @Test
  public void testTypeCtor() throws InvalidValueException {
    NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
    assertEquals(0, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_TINYINT);
    assertEquals(Byte.MAX_VALUE, n.getLongValue());
    assertEquals(Type.TINYINT, n.getType());

    n = new NumericLiteral(NumericLiteral.MAX_BIGINT);
    assertEquals(Type.BIGINT, n.getType());
    assertEquals(Long.MAX_VALUE, n.getLongValue());

    n = new NumericLiteral(NumericLiteral.MAX_DOUBLE);
    assertEquals(Double.MAX_VALUE, n.getDoubleValue(), 1.0);
    assertEquals(Type.DOUBLE, n.getType());

    n = new NumericLiteral(new BigDecimal(genDecimal(35, 0)));
    assertEquals(ScalarType.createDecimalType(35, 0), n.getType());

    try {
      new NumericLiteral(NumericLiteral.MAX_DOUBLE.multiply(BigDecimal.TEN));
      fail();
    } catch(InvalidValueException e) {
      // Expected
    }
  }

  @Test
  public void testSimpleCtor2() throws InvalidValueException {
  }

  @Test
  public void testCastTo() throws AnalysisException {
    // Integral types
    {
      NumericLiteral n = new NumericLiteral(BigDecimal.ZERO);
      Expr result = n.uncheckedCastTo(Type.BIGINT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      result = n.uncheckedCastTo(Type.TINYINT);
      assertSame(n, result);
      assertEquals(Type.TINYINT, n.getType());
      result = n.uncheckedCastTo(ScalarType.createDecimalType(5, 0));
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(5, 0), n.getType());
    }
    // Integral types, with overflow
    {
      NumericLiteral n = new NumericLiteral(ABOVE_SMALLINT);
      Expr result = n.uncheckedCastTo(Type.BIGINT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      try {
        n.uncheckedCastTo(Type.SMALLINT);
        fail();
      } catch (SqlCastException e) {
        // Expected
      }
    }
    // Decimal types, with overflow
    // Note: not safe to reuse above value after exception
    {
      NumericLiteral n = new NumericLiteral(ABOVE_SMALLINT);
      Expr result = n.uncheckedCastTo(Type.BIGINT);
      assertSame(n, result);
      assertEquals(Type.BIGINT, n.getType());
      try {
        n.uncheckedCastTo(ScalarType.createDecimalType(2, 0));
        fail();
      } catch (SqlCastException e) {
        // Expected
      }
    }
    // Decimal types
    {
      NumericLiteral n = new NumericLiteral(new BigDecimal("123.45"));
      assertEquals(ScalarType.createDecimalType(5, 2), n.getType());
      Expr result = n.uncheckedCastTo(ScalarType.createDecimalType(6, 3));
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(6, 3), n.getType());
      result = n.uncheckedCastTo(ScalarType.createDecimalType(5, 2));
      assertSame(n, result);
      assertEquals(ScalarType.createDecimalType(5, 2), n.getType());
      result = n.uncheckedCastTo(ScalarType.createDecimalType(4, 1));
      assertNotSame(n, result);
      assertEquals(ScalarType.createDecimalType(4, 1), result.getType());
      assertEquals("123.5", ((NumericLiteral) result).toSql());
    }
  }

}
