// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.common.SqlCastException;
import org.junit.Test;

public class LiteralTest {

  @Test
  public void testNullLiteralBasics() throws AnalysisException {
    // Null of the default NULL type
    NullLiteral nullNull = new NullLiteral();
    assertEquals(Type.NULL, nullNull.getType());
    assertTrue(nullNull.isAnalyzed());
    assertEquals(LiteralExpr.LITERAL_COST, nullNull.getCost(), 0.01);
    assertEquals(-1, nullNull.getSelectivity(), 0.01);
    assertEquals("NULL", nullNull.getStringValue());
    assertEquals("NULL:NULL_TYPE", nullNull.toString());

    // Null of an explicit type of BIGINT
    NullLiteral nullBigInt = NullLiteral.create(Type.BIGINT);
    assertEquals(Type.BIGINT, nullBigInt.getType());
    assertTrue(nullBigInt.isAnalyzed());
    assertEquals(LiteralExpr.LITERAL_COST, nullBigInt.getCost(), 0.01);
    assertEquals(-1, nullBigInt.getSelectivity(), 0.01);
    assertEquals("NULL", nullBigInt.getStringValue());
    assertEquals("NULL:BIGINT", nullBigInt.toString());

    // Clone
    Expr n3 = nullBigInt.clone();
    assertEquals(Type.BIGINT, n3.getType());
  }

  @Test
  public void testNullCompare() {
    NullLiteral nullNull = new NullLiteral();
    NullLiteral nullBigInt = NullLiteral.create(Type.BIGINT);

    // Comparisons
    NumericLiteral n = NumericLiteral.create(0);
    // Nulls are equal
    assertEquals(0, nullBigInt.compareTo(nullBigInt));
    assertNotEquals(0, nullBigInt.compareTo(nullNull));
    assertEquals(0, nullNull.compareTo(new NullLiteral()));
    assertNotEquals(0, nullNull.compareTo(nullBigInt));
    assertEquals(Integer.signum(nullBigInt.compareTo(nullNull)),
        -Integer.signum(nullNull.compareTo(nullBigInt)));
    // Nulls before other types
    assertEquals(-1, nullBigInt.compareTo(n));
    assertEquals(1, n.compareTo(nullBigInt));
    // Different nulls have different hash codes
    // Not sure if hashCode() is ever used...
    assertNotEquals(nullNull.hashCode(), nullBigInt.hashCode());
    assertEquals(nullNull.hashCode(), new NullLiteral().hashCode());
    // If equals() must return true, hash code must be identical
    //sassertFalse(nullNull.equals(nullBigInt));
    assertTrue(nullNull.equals(new NullLiteral()));
  }

  @Test
  public void testNullCast() throws AnalysisException {

    // Explicit/implicit cast
    NullLiteral null3 = new NullLiteral();
    assertEquals(Type.NULL, null3.getType());
    // Implicit cast
    null3.castTo(Type.INT);
    assertEquals(Type.INT, null3.getType());

    // Unchecked cast
    NullLiteral null4 = new NullLiteral();
    Expr result = null4.uncheckedCastTo(Type.INT);
    assertSame(null4, result);
    assertEquals(Type.INT, null4.getType());
  }

  @Test
  public void testBooleanLiteralBasics() throws AnalysisException {
    BoolLiteral bool = BoolLiteral.create(true);
    assertTrue(bool.getValue());
    assertEquals(Type.BOOLEAN, bool.getType());
    assertTrue(bool.isAnalyzed());
    assertEquals(LiteralExpr.LITERAL_COST, bool.getCost(), 0.01);
    assertEquals(-1, bool.getSelectivity(), 0.01);
    assertEquals("TRUE", bool.getStringValue());
    assertEquals("TRUE", bool.toString());

    BoolLiteral boolTrue = BoolLiteral.create("True");
    assertTrue(boolTrue.getValue());
    BoolLiteral boolFalse = BoolLiteral.create("fAlSe");
    assertFalse(boolFalse.getValue());
    try {
      BoolLiteral.create("1");
      fail();
    } catch (InvalidValueException e) {
      // Expected
    }

    // Clone
    Expr n3 = bool.clone();
    assertEquals(Type.BOOLEAN, n3.getType());
    assertTrue(n3 instanceof BoolLiteral);
    assertTrue(((BoolLiteral) n3).getValue());
  }

  @Test
  public void testBooleanCompare() {
    BoolLiteral boolTrue = new BoolLiteral(true);
    BoolLiteral boolFalse = new BoolLiteral(false);

    // Comparisons
    // Nulls before other types
    NullLiteral nullBool = new NullLiteral(Type.BOOLEAN);
    assertEquals(-1, nullBool.compareTo(boolTrue));
    assertEquals(1, boolTrue.compareTo(nullBool));
    // Equality
    BoolLiteral true2 = new BoolLiteral(true);
    assertEquals(0, true2.compareTo(boolTrue));
    assertEquals(0, boolTrue.compareTo(true2));
    assertEquals(-1, boolFalse.compareTo(boolTrue));
    assertEquals(1, boolTrue.compareTo(boolFalse));

    // Different nulls have different hash codes
    // Not sure if hashCode() is ever used...
    assertNotEquals(boolTrue.hashCode(), boolFalse.hashCode());
    assertEquals(boolTrue.hashCode(), true2.hashCode());
    assertNotEquals(boolFalse.hashCode(), new BoolLiteral(false));
    // If equals() must return true, hash code must be identical
    assertFalse(boolTrue.equals(boolFalse));
    assertTrue(boolTrue.equals(true2));
    assertFalse(boolTrue.localEquals(boolFalse));
    assertTrue(boolTrue.localEquals(true2));
  }

  @Test
  public void testBooleanCast() throws AnalysisException {

    // Unchecked cast
    BoolLiteral bool = new BoolLiteral(true);
    Expr result = bool.uncheckedCastTo(Type.BOOLEAN);
    assertSame(bool, result);
    result = bool.uncheckedCastTo(Type.INT);
    assertTrue(result instanceof CastExpr);
  }

  @Test
  public void testStringLiteralBasics() throws AnalysisException {
    StringLiteral str = new StringLiteral("foo");
    assertEquals("foo", str.getStringValue());
    assertEquals(Type.STRING, str.getType());
    assertTrue(str.isAnalyzed());
    assertEquals(LiteralExpr.LITERAL_COST, str.getCost(), 0.01);
    assertEquals(-1, str.getSelectivity(), 0.01);
    assertEquals("'foo':STRING", str.toString());
    assertTrue(str.needsUnescaping());

    Expr expr = str.clone();
    assertTrue(expr instanceof StringLiteral);
    StringLiteral s2 = (StringLiteral) expr;
    assertEquals("foo", s2.getStringValue());
    assertEquals(Type.STRING, s2.getType());
    assertTrue(s2.isAnalyzed());
    assertEquals(LiteralExpr.LITERAL_COST, s2.getCost(), 0.01);
    assertEquals(-1, s2.getSelectivity(), 0.01);

    StringLiteral s3 = new StringLiteral("bar");
    assertTrue(str.equals(s2));
    assertFalse(str.equals(s3));
    assertTrue(str.localEquals(s2));
    assertFalse(str.localEquals(s3));
    assertEquals(0, str.compareTo(s2));
    assertTrue(str.compareTo(s3) > 0);
    assertTrue(s3.compareTo(str) < 0);
  }

  private void testQuotes(String orig, String normalized, String unescaped) {
    StringLiteral str = new StringLiteral(orig);
    assertEquals(orig, str.getValueWithOriginalEscapes());
    assertEquals(normalized, str.getNormalizedValue());
    assertEquals(unescaped, str.getUnescapedValue());
  }

  @Test
  public void testStringLiteralQuotes() throws AnalysisException {
    testQuotes("a'b", "a\\'b", "a'b");
    testQuotes("a\\'b", "a\\'b", "a'b");
    testQuotes("a\"b", "a\"b", "a\"b");
    testQuotes("a\\\"b", "a\"b", "a\"b");
    testQuotes("a\\\\b", "a\\\\b", "a\\b");
    testQuotes("a\\nb", "a\\nb", "a\nb");
  }

  @Test
  public void testStringToNumber() throws AnalysisException {
    // Full testing of conversion is in NumericLiteralTest.testStringToNumber()
    StringLiteral s = new StringLiteral("0");
    LiteralExpr expr = s.convertToNumber();
    assertTrue(expr instanceof NumericLiteral);
    NumericLiteral n = (NumericLiteral) expr;
    assertEquals(0, n.getIntValue());
    assertEquals(Type.TINYINT, n.getType());
  }

  @Test
  public void testStringCast() throws AnalysisException {
    StringLiteral s = new StringLiteral("1234");
    Expr expr = s.uncheckedCastTo(Type.TINYINT);
//    assertTrue(expr instanceof CastExpr);
//    assertEquals(Type.TINYINT, expr.getType());
    expr = s.uncheckedCastTo(Type.SMALLINT);
    assertTrue(expr instanceof NumericLiteral);
    assertEquals(Type.SMALLINT, expr.getType());
    expr = s.uncheckedCastTo(Type.STRING);
    assertSame(s, expr);
    expr = s.uncheckedCastTo(ScalarType.createVarcharType());
    assertSame(s, expr);
    assertEquals(ScalarType.createVarcharType(), expr.getType());
//    try {
//      s.uncheckedCastTo(Type.BOOLEAN);
//      fail();
//    } catch(SqlCastException e) {
//      // Expected
//    }
  }
}
