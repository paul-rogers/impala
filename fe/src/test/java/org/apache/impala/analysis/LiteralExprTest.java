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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.junit.Test;

import com.google.common.collect.Lists;


public class LiteralExprTest extends FrontendTestBase {
  // Test creation of LiteralExprs from Strings, e.g., for partitioning keys.
  @Test
  public void TestLiteralExpr() {
    testLiteralExprPositive("false", Type.BOOLEAN);
    testLiteralExprPositive("1", Type.TINYINT);
    testLiteralExprPositive("1", Type.SMALLINT);
    testLiteralExprPositive("1", Type.INT);
    testLiteralExprPositive("1", Type.BIGINT);
    testLiteralExprPositive("1.0", Type.FLOAT);
    testLiteralExprPositive("1.0", Type.DOUBLE);
    testLiteralExprPositive("ABC", Type.STRING);
    testLiteralExprPositive("1.1", ScalarType.createDecimalType(2, 1));

    // INVALID_TYPE should always fail
    testLiteralExprNegative("ABC", Type.INVALID);

    // Invalid casts
    testLiteralExprNegative("ABC", Type.BOOLEAN);
    testLiteralExprNegative("ABC", Type.TINYINT);
    testLiteralExprNegative("ABC", Type.SMALLINT);
    testLiteralExprNegative("ABC", Type.INT);
    testLiteralExprNegative("ABC", Type.BIGINT);
    testLiteralExprNegative("ABC", Type.FLOAT);
    testLiteralExprNegative("ABC", Type.DOUBLE);
    testLiteralExprNegative("ABC", Type.TIMESTAMP);
    testLiteralExprNegative("ABC", ScalarType.createDecimalType());

    // Date types not implemented
    testLiteralExprNegative("2010-01-01", Type.DATE);
    testLiteralExprNegative("2010-01-01", Type.DATETIME);
    testLiteralExprNegative("2010-01-01", Type.TIMESTAMP);
  }

  private void testLiteralExprPositive(String value, Type type) {
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.create(value, type);
    } catch (Exception e) {
      fail("\nFailed to create LiteralExpr of type: " + type.toString() +
          " from: " + value + " due to " + e.getMessage() + "\n");
    }
    if (expr == null) {
      fail("\nFailed to create LiteralExpr\n");
    }
  }

  private void testLiteralExprNegative(String value, Type type) {
    boolean failure = false;
    LiteralExpr expr = null;
    try {
      expr = LiteralExpr.create(value, type);
    } catch (Exception e) {
      failure = true;
    }
    if (expr == null) {
      failure = true;
    }
    if (!failure) {
      fail("\nUnexpectedly succeeded to create LiteralExpr of type: "
          + type.toString() + " from: " + value + "\n");
    }
  }

  private Expr analyze(String query, boolean useDecimalV2, boolean enableRewrite) {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryOptions().setDecimal_v2(useDecimalV2);
    ctx.getQueryOptions().setEnable_expr_rewrites(enableRewrite);
    return ((SelectStmt) AnalyzesOk(query, ctx)).getSelectList()
        .getItems().get(0).getExpr();
  }

  /**
   * Test extreme literal cases to ensure the value passes
   * through the analyzer correctly.
   */
  @Test
  public void testLiteralCast() {
    for (int i = 0; i < 3; i++) {
      boolean useDecimalV2 = i > 1;
      boolean enableRewrite = (i % 2) == 1;
      {
        // Boundary case in which the positive value is a DECIMAL,
        // becomes BIGINT when negated by the parser.
        String query = "select getBit(" + Long.MIN_VALUE + ", 63)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.TINYINT, expr.getType());
      }
      {
        // Would eval to NaN, so keep original expr.
        String query = "select cast(10 as double) / cast(0 as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        assertTrue(expr instanceof ArithmeticExpr);
      }
      {
        // Extreme double value. Ensure double-->BigDecimal noise
        // does not cause overflows
        String query = "select cast(" + Double.toString(Double.MAX_VALUE) +
            " as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        if (enableRewrite) {
          assertTrue(expr instanceof NumericLiteral);
        } else {
          assertTrue(expr instanceof CastExpr);
        }
      }
      {
        // As above, but for extreme minimum (smallest) values
        String query = "select cast(" + Double.toString(Double.MIN_VALUE) +
            " as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        if (enableRewrite) {
          assertTrue(expr instanceof NumericLiteral);
        } else {
          assertTrue(expr instanceof CastExpr);
        }
      }
      {
        // Math would cause overflow, don't rewrite
        String query = "select cast(1.7976931348623157e+308 as double)" +
            " / cast(2.2250738585072014e-308 as double)";
        Expr expr = analyze(query, useDecimalV2, enableRewrite);
        assertEquals(Type.DOUBLE, expr.getType());
        assertTrue(expr instanceof ArithmeticExpr);
      }
    }
  }

  private Analyzer prepareAnalyzer() throws ImpalaException {
    AnalysisContext ctx = createAnalysisCtx();
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(frontend_, ctx.getQueryCtx().session.database, null);
    StmtTableCache loadedTables = mdLoader.loadTables(Sets.newHashSet(
        new TableName("functional", "alltypes")));
    Analyzer analyzer = ctx.createAnalyzer(loadedTables);
    TableRef tableRef = analyzer.resolveTableRef(
        new TableRef(Lists.newArrayList("functional", "alltypes"), null));
    tableRef.analyze(analyzer);
    return analyzer;
  }


  private LiteralExpr eval(String exprSql) throws ImpalaException {
    Analyzer analyzer = prepareAnalyzer();

    String stmtSql = "select " + exprSql + " from functional.alltypes";
    Expr expr = ((SelectStmt) ParsesOk(stmtSql)).getSelectList().getItems().get(0).getExpr();
    assertFalse(expr instanceof LiteralExpr);
    analyzer.analyzeSafe(expr);

    return LiteralExpr.create(expr, analyzer.getQueryCtx());
  }

  private void verifyEval(Type type, String exprSql) throws ImpalaException {
    assertEquals(type, eval(exprSql).getType());
  }

  @Test
  public void testEval() throws ImpalaException {
    verifyEval(Type.SMALLINT, "1 + 1");
    verifyEval(Type.INT, "1 + 256");
    verifyEval(Type.INT, "CAST(1 AS INT)");
    verifyEval(Type.TIMESTAMP, "CAST('2015-11-15' AS TIMESTAMP)");
    verifyEval(Type.TIMESTAMP, "CAST('2016-11-20 00:00:00' AS TIMESTAMP)");
    verifyEval(Type.TIMESTAMP, "CAST('2015-11-15' AS TIMESTAMP) + INTERVAL 1 year");
    verifyEval(Type.BOOLEAN, "2 > 1");
    verifyEval(Type.STRING, "concat('a', 'b')");

    // Boolean can be cast to multiple types
    {
      LiteralExpr expr = eval("cast(true as tinyint)");
      assertEquals(Type.TINYINT, expr.getType());
      assertEquals(1, ((NumericLiteral) expr).getIntValue());
    }
    {
      LiteralExpr expr = eval("cast(true as int)");
      assertEquals(Type.INT, expr.getType());
      assertEquals(1, ((NumericLiteral) expr).getIntValue());
    }
    {
      LiteralExpr expr = eval("cast(true as string)");
      assertEquals(ScalarType.STRING, expr.getType());
      assertEquals("1", ((StringLiteral) expr).getUnescapedValue());
    }
    // Cast of BOOLEAN to DECIMAL not supported

    // BE will wrap invalid integers. More of a bug than a feature,
    // but is related to how C++ does math.
    {
      LiteralExpr expr = eval("cast(257 as tinyint)");
      assertEquals(Type.TINYINT, expr.getType());
      assertEquals(1, ((NumericLiteral) expr).getIntValue());
    }
  }
}
