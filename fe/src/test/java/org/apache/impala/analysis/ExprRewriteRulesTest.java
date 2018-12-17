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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.ExprAnalyzer.RewriteMode;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.SqlCastException;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {

  private Analyzer prepareAnalyzer(boolean enableRewrites) throws ImpalaException {
    AnalysisContext ctx = createAnalysisCtx();
    ctx.getQueryCtx().client_request.query_options.setEnable_expr_rewrites(enableRewrites);
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

  private Expr parseSelectExpr(String tableName, String exprSql) {
    String stmtSql = "select " + exprSql + " from " + tableName;
    return ((SelectStmt) ParsesOk(stmtSql)).getSelectList().getItems().get(0).getExpr();
  }

  private Expr parseSelectExpr(String exprSql) {
    return parseSelectExpr("functional.alltypes", exprSql);
  }

  private Expr analyzeSelectExpr(Expr expr) throws ImpalaException {
    return prepareAnalyzer(true).analyzeAndRewrite(expr);
  }

  private Expr analyzeWithRewrite(String exprSql) throws ImpalaException {
    return prepareAnalyzer(true).analyzeAndRewrite(
        parseSelectExpr(exprSql));
  }

  private Expr analyzeWithoutRewrite(String exprSql) throws ImpalaException {
    return prepareAnalyzer(false).analyzeAndRewrite(
        parseSelectExpr(exprSql));
  }

  private Expr verifyRewrite(String exprSql, String expectedSql) throws ImpalaException {
    Expr expr = parseSelectExpr(exprSql);
    Analyzer analyzer = prepareAnalyzer(true);
    Expr result = analyzer.analyzeAndRewrite(expr);
    if (analyzer.exprAnalyzer().transformCount() == 0) {
      assertNull("Expected rewrite", expectedSql);
      assertSame(expr, result);
    } else {
      if (expectedSql == null) System.out.println(result.toSql());
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
    return result;
  }

  private Expr verifyWhereRewrite(String exprSql, String expectedSql) throws ImpalaException {
    String stmtSql = "select count(1) from functional.alltypes where " + exprSql;
    Expr expr = ((SelectStmt) ParsesOk(stmtSql)).getWhereClause();
    Analyzer analyzer = prepareAnalyzer(true);
    Expr result = analyzer.analyzeAndRewrite(expr);
    if (analyzer.exprAnalyzer().transformCount() == 0) {
      assertNull("Expected rewrite", expectedSql);
      assertSame(expr, result);
    } else {
      if (expectedSql == null) System.out.println(result.toSql());
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
    return result;
  }

  private void verifySingleRewrite(String tableName, String exprSql,
      Class<? extends Expr> nodeClass,
      String expectedSql) throws ImpalaException {
    Analyzer analyzer = prepareAnalyzer(false);
    Expr expr = analyzer.analyzeAndRewrite(parseSelectExpr(tableName, exprSql));
    assertTrue(nodeClass.isInstance(expr));
    Expr result = expr.rewrite(analyzer.exprAnalyzer());
    if (result == expr) {
      assertNull("Expected rewrite", expectedSql);
    } else {
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
  }

  private Expr verifySingleRewrite(String exprSql, Class<? extends Expr> nodeClass,
      String expectedSql) throws ImpalaException {
    Expr original = parseSelectExpr(exprSql);
    assertTrue(nodeClass.isInstance(original));
    Analyzer analyzer = prepareAnalyzer(false);
    Expr expr = analyzer.analyzeAndRewrite(original);
    ExprAnalyzer exprAnalyzer = new ExprAnalyzer(analyzer, RewriteMode.OPTIONAL);
    Expr result = expr.rewrite(exprAnalyzer);
    if (result == original) {
      assertNull("Expected rewrite", expectedSql);
    } else {
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
    return result;
  }

  private void verifyRewriteType(String exprSql, Type expectedType, String expectedSql)
      throws ImpalaException {
    Expr expr = verifyRewrite(exprSql, expectedSql);
    assertEquals(expectedType, expr.getType());
  }

  /**
   * Verify internal structure of a BETWEEEN rewrite
   */
  @Test
  public void testBetweenToCompoundDeep() throws ImpalaException {
    {
      Expr between = parseSelectExpr("int_col between 1 and 10");
      assertTrue(between instanceof BetweenPredicate);

      Expr result = analyzeSelectExpr(between);
      assertEquals("int_col >= 1 AND int_col <= 10", result.toSql());
      verifyBetweenRewrite(between, result);
    }
    {
      Expr between = parseSelectExpr("int_col not between 1 and 10");
      assertTrue(between instanceof BetweenPredicate);

      Expr result = analyzeSelectExpr(between);
      assertEquals("(int_col < 1 OR int_col > 10)", result.toSql());
      verifyBetweenRewrite(between, result);
    }
  }

  private void verifyBetweenRewrite(Expr between, Expr result) {
    Expr intCol = between.getChild(0);
    Expr lowBound = between.getChild(1);
    Expr highBound = between.getChild(2);

    assertTrue(result instanceof CompoundPredicate);
    assertTrue(result.isAnalyzed());
    assertEquals(Type.BOOLEAN, result.getType());
    assertEquals(3, result.getNumDistinctValues());
    assertEquals(7.0, result.getCost(), 0.001);

    Expr left = result.getChild(0);
    assertTrue(left instanceof BinaryPredicate);
    assertEquals(Type.BOOLEAN, left.getType());
    assertEquals(3, left.getNumDistinctValues());
    assertEquals(3.0, left.getCost(), 0.001);
    // TODO: Fix this, use industry standard guess
    assertEquals(-1, left.getSelectivity(), 0.01);
    assertSame(intCol, left.getChild(0));
    assertSame(lowBound, left.getChild(1));

    Expr right = result.getChild(1);
    assertTrue(right instanceof BinaryPredicate);
    assertEquals(Type.BOOLEAN, right.getType());
    assertEquals(3, right.getNumDistinctValues());
    assertEquals(3.0, right.getCost(), 0.001);
    // TODO: Fix this, use industry standard guess
    assertEquals(-1, right.getSelectivity(), 0.01);
    assertSame(intCol, right.getChild(0));
    assertSame(highBound, right.getChild(1));
  }

  @Test
  public void testBetweenToCompoundRule() throws ImpalaException {
    Class<? extends Expr> nodeClass = BetweenPredicate.class;

    // Basic BETWEEN predicates.
    verifySingleRewrite("int_col between float_col and double_col",
        nodeClass,
        "int_col >= float_col AND int_col <= double_col");
    verifySingleRewrite("int_col not between float_col and double_col",
        nodeClass,
        "(int_col < float_col OR int_col > double_col)");
    verifySingleRewrite("50.0 between null and 5000",
        nodeClass, "50.0 >= NULL AND 50.0 <= 5000");
    // Basic NOT BETWEEN predicates.
    verifySingleRewrite("int_col between 10 and 20",
        nodeClass, "int_col >= 10 AND int_col <= 20");
    verifySingleRewrite("int_col not between 10 and 20",
        nodeClass, "(int_col < 10 OR int_col > 20)");
    verifySingleRewrite("50.0 not between null and 5000",
        nodeClass, "(50.0 < NULL OR 50.0 > 5000)");

    // Rewrites don't change perceived precedence
    verifyRewrite(
        "int_col not between 1 and 10 and int_col <> -1",
        "(int_col < 1 OR int_col > 10) AND int_col != -1");

    // Nested BETWEEN predicates.
    verifyRewrite(
        "int_col between if(tinyint_col between 1 and 2, 10, 20) " +
        "and cast(smallint_col between 1 and 2 as int)",
        "int_col >= if(tinyint_col >= 1 AND tinyint_col <= 2, 10, 20) " +
        "AND int_col <= CAST(smallint_col >= 1 AND smallint_col <= 2 AS INT)");
    // Nested NOT BETWEEN predicates.
    verifyRewrite(
        "int_col not between if(tinyint_col not between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)",
        "(int_col < if((tinyint_col < 1 OR tinyint_col > 2), 10, 20) " +
        "OR int_col > CAST((smallint_col < 1 OR smallint_col > 2) AS INT))");
    // Mixed nested BETWEEN and NOT BETWEEN predicates.
    verifyRewrite(
        "int_col between if(tinyint_col between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)",
        "int_col >= if(tinyint_col >= 1 AND tinyint_col <= 2, 10, 20) " +
        "AND int_col <= CAST((smallint_col < 1 OR smallint_col > 2) AS INT)");
  }

  @Test
  public void testExtractCommonConjunctsRule() throws ImpalaException {
    Class<? extends Expr> nodeClass = CompoundPredicate.class;

    // One common conjunct: int_col < 10
    verifySingleRewrite(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10)", nodeClass,
        "int_col < 10 AND ((bigint_col < 10) OR (string_col = '10'))");
    // One common conjunct in multiple disjuncts: int_col < 10
    verifyRewrite(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and float_col > 3.14)",
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Same as above but with a bushy OR tree.
    verifyRewrite(
        "((int_col < 10 and bigint_col < 10) or " +
        " (string_col = '10' and int_col < 10)) or " +
        "((id < 20 and int_col < 10) or " +
        " (int_col < 10 and float_col > 3.14))",
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Multiple common conjuncts: int_col < 10, bool_col is null
    verifySingleRewrite(
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(bool_col is null and string_col = '10' and int_col < 10)", nodeClass,
        "int_col < 10 AND bool_col IS NULL AND " +
        "((bigint_col < 10) OR (string_col = '10'))");
    // Negated common conjunct: !(int_col=5 or tinyint_col > 9)
    verifySingleRewrite(
        "(!(int_col=5 or tinyint_col > 9) and double_col = 7) or " +
        "(!(int_col=5 or tinyint_col > 9) and double_col = 8)", nodeClass,
        "NOT (int_col = 5 OR tinyint_col > 9) AND " +
        "((double_col = 7) OR (double_col = 8))");

    // Test common BetweenPredicate: int_col between 10 and 30
    verifyRewrite(
        "(int_col between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)",
        "int_col >= 10 AND int_col <= 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test common NOT BetweenPredicate: int_col not between 10 and 30
    verifyRewrite(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col not between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col not between 10 and 30 and float_col > 3.14)",
        "int_col < 10 OR int_col > 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test mixed BetweenPredicates are not common.
    verifyRewrite(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)",
        "((int_col < 10 OR int_col > 30) AND bigint_col < 10) OR "+
        "(string_col = '10' AND int_col >= 10 AND int_col <= 30) OR " +
        "(id < 20 AND (int_col < 10 OR int_col > 30)) OR " +
        "(int_col >= 10 AND int_col <= 30 AND float_col > 3.14)");

    // All conjuncts are common.
    verifyRewrite(
        "(int_col < 10 and id between 5 and 6) or " +
        "(id between 5 and 6 and int_col < 10) or " +
        "(int_col < 10 and id between 5 and 6)",
        "int_col < 10 AND id >= 5 AND id <= 6");
    // Complex disjuncts are redundant.
    verifyRewrite(
        "(int_col < 10) or " +
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(int_col < 10) or " +
        "(bool_col is null and int_col < 10)",
        "int_col < 10");

    // Due to the shape of the original OR tree we are left with redundant
    // disjuncts after the extraction.
    verifyRewrite(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and id < 20)",
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR (id < 20) OR (id < 20))");
  }

  @Test
  public void testFoldConstantsDeep() throws ImpalaException {
    {
      // Sanity check: no rewrite if rewrites disabled
      Expr expr = analyzeWithoutRewrite("1 + 1");
      assertEquals("1 + 1", expr.toSql());
    }
    {
      // Push null type into null literal
      Expr expr = analyzeWithRewrite("CAST(NULL AS INT)");
      assertTrue(expr instanceof NullLiteral);
      assertTrue(expr.isAnalyzed());
      assertEquals(Type.INT, expr.getType());
      assertEquals(1.0, expr.getCost(), 0.01);
      assertEquals(1, expr.getNumDistinctValues());
    }
    {
      // Push type into numeric literal
      Expr expr = analyzeWithRewrite("CAST(10 AS INT)");
      assertTrue(expr instanceof NumericLiteral);
      assertTrue(expr.isAnalyzed());
      assertEquals(Type.INT, expr.getType());
      assertEquals(1.0, expr.getCost(), 0.01);
      assertEquals(1, expr.getNumDistinctValues());
    }
    {
      // Simple constant folding - untyped
      Expr expr = analyzeWithRewrite("1 + 1");
      assertTrue(expr instanceof NumericLiteral);
      assertTrue(expr.isAnalyzed());
      assertEquals(Type.TINYINT, expr.getType());
      assertEquals(1.0, expr.getCost(), 0.01);
      assertEquals(1, expr.getNumDistinctValues());
    }
    {
      // Simple constant folding - typed
      Expr expr = analyzeWithRewrite("CAST(1 AS TINYINT) + 1");
      assertTrue(expr instanceof NumericLiteral);
      assertTrue(expr.isAnalyzed());
      assertEquals(Type.SMALLINT, expr.getType());
      assertEquals(1.0, expr.getCost(), 0.01);
      assertEquals(1, expr.getNumDistinctValues());
    }
  }

  /**
   * Only contains very basic tests for a few interesting cases. More thorough
   * testing is done in expr-test.cc.
   */
  @Test
  public void testFoldConstants() throws ImpalaException {
    verifyRewriteType("1 + 1", Type.TINYINT, "2");
    verifyRewriteType("1 + 1 + 1 + 1 + 1", Type.TINYINT, "5");
    verifyRewriteType("10 - 5 - 2 - 1 - 8", Type.TINYINT, "-6");
    verifyRewriteType("CAST(1 as int) + 1", Type.BIGINT, "2");
    verifyRewriteType("factorial(4)", Type.TINYINT, "24");
    verifyRewriteType("factorial(CAST(4 AS TINYINT))", Type.BIGINT, "24");
    verifyRewrite("cast('2016-11-09' as timestamp)",
        "TIMESTAMP '2016-11-09 00:00:00'");
    verifyRewrite("cast('2016-11-09' as timestamp) + interval 1 year",
        "TIMESTAMP '2017-11-09 00:00:00'");
    // Tests that exprs that warn during their evaluation are not folded.
    verifyRewrite("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1 DAYS",
        "TIMESTAMP '9999-12-31 21:00:00' + INTERVAL 1 DAYS");

    // Tests correct handling of strings with escape sequences.
    verifyRewrite("'_' LIKE '\\\\_'", "TRUE");
    verifyRewrite("base64decode(base64encode('\\047\\001\\132\\060')) = " +
      "'\\047\\001\\132\\060'", "TRUE");

    // Tests correct handling of strings with chars > 127. Should not be folded.
    // Eval done on BE, ASCII returned
    verifyRewrite("hex(unhex('D3'))", "'D3'");
    verifyRewrite("hex(unhex(hex(unhex('D3'))))", "'D3'");
    // Non-ASCII returned to FE
    verifyRewrite("unhex('D3')", null);
    verifyRewrite("unhex(hex(unhex('D3')))", "unhex('D3')");
    // Tests that non-deterministic functions are not folded.
    verifyRewrite("rand()", null);
    verifyRewrite("random()", null);
    verifyRewrite("uuid()", null);

    verifyRewrite("null + 1", "NULL");
    verifyRewrite("(1 + 1) is null", "FALSE");
    verifyRewrite("(null + 1) is null", "TRUE");
  }

  @Test
  public void testSimplifyIf() throws ImpalaException {
    Class<? extends Expr> nodeClass = FunctionCallExpr.class;

    // IF
    verifySingleRewrite("if(true, id, id+1)", nodeClass, "id");
    verifySingleRewrite("if(false, id, id+1)", nodeClass, "id + 1");
    verifySingleRewrite("if(null, id, id+1)", nodeClass, "id + 1");
    verifySingleRewrite("if(id = 0, true, false)", nodeClass, null);

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    verifyRewrite("if(true, 0, sum(id))", null);
    verifyRewrite("if(false, sum(id), 0)", null);
    verifyRewrite("if(null, sum(id), 0)", null);
    verifyRewrite("if(false, max(id), min(id))", "min(id)");
  }

  @Test
  public void testSimplifyIfNull() throws ImpalaException {
    Class<? extends Expr> nodeClass = FunctionCallExpr.class;

    // IFNULL and its aliases
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      verifySingleRewrite(f + "(null, id)", nodeClass, "id");
      verifySingleRewrite(f + "(null, null)", nodeClass, "NULL");
      verifySingleRewrite(f + "(id, id + 1)", nodeClass, null);

      verifySingleRewrite(f + "(1, 2)", nodeClass, "1");
      verifySingleRewrite(f + "(0, id)", nodeClass, "0");
      // non literal constants shouldn't be simplified by the rule
      verifySingleRewrite(f + "(1 + 1, id)", nodeClass, null);
      verifySingleRewrite(f + "(NULL + 1, id)", nodeClass, null);
    }

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    verifyRewrite("ifnull(null, max(id))", "max(id)");
    verifyRewrite("ifnull(1, max(id))", null);
  }

  @Test
  public void testSimplifyCompoundPredicate() throws ImpalaException {
    Class<? extends Expr> nodeClass = CompoundPredicate.class;

    verifySingleRewrite("false || id = 0", nodeClass, "id = 0");
    verifySingleRewrite("true || id = 0", nodeClass, "TRUE");
    verifySingleRewrite("false && id = 0", nodeClass, "FALSE");
    verifySingleRewrite("true && id = 0", nodeClass, "id = 0");
    // NULL with a non-constant other child doesn't get rewritten.
    verifySingleRewrite("null && id = 0", nodeClass, null);
    verifySingleRewrite("null || id = 0", nodeClass, null);
    // Normalization happens first
    verifySingleRewrite("id = 0 || false", nodeClass, "FALSE OR id = 0");

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    verifyRewrite("true || sum(id) = 0", null);
    verifyRewrite("not true", "FALSE");
    verifyRewrite("not false", "TRUE");
    verifyRewrite("id = 0 || false", "id = 0");
  }

  @Test
  public void testSimplifyCaseWithValue() throws ImpalaException {
    Class<? extends Expr> nodeClass = CaseExpr.class;

    // Single TRUE case with no preceding non-constant cases.
    verifySingleRewrite("case 1 when 0 then id when 1 then id + 1 when 2 then id + 2 end",
        nodeClass, "id + 1");
    // SINGLE TRUE case with preceding non-constant case.
    verifySingleRewrite("case 1 when id then id when 1 then id + 1 end", nodeClass,
        "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // Single FALSE case.
    verifySingleRewrite("case 0 when 1 then 1 when id then id + 1 end", nodeClass,
        "CASE 0 WHEN id THEN id + 1 END");
    // All FALSE, return ELSE.
    verifySingleRewrite("case 2 when 0 then id when 1 then id * 2 else 0 end", nodeClass, "0");
    // All FALSE, return implicit NULL ELSE.
    verifySingleRewrite("case 3 when 0 then id when 1 then id + 1 end", nodeClass, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    verifyRewrite("case 1 when id then id when 2 - 1 then id + 1 when 1 then id + 2 end",
        "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // When NULL.
    verifySingleRewrite("case 0 when null then id else 1 end", nodeClass, "1");
    // All non-constant, don't rewrite.
    verifySingleRewrite("case id when 1 then 1 when 2 then 2 else 3 end", nodeClass, null);
  }

  @Test
  public void testSimplifyCaseWithoutValue() throws ImpalaException {
    Class<? extends Expr> nodeClass = CaseExpr.class;

    // Single TRUE case with no predecing non-constant case.
    verifySingleRewrite("case when FALSE then 0 when TRUE then 1 end", nodeClass, "1");
    // Single TRUE case with preceding non-constant case.
    verifySingleRewrite("case when id = 0 then 0 when true then 1 when id = 2 then 2 end", nodeClass,
        "CASE WHEN id = 0 THEN 0 ELSE 1 END");
    // Single FALSE case.
    verifySingleRewrite("case when id = 0 then 0 when false then 1 when id = 2 then 2 end", nodeClass,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");
    // All FALSE, return ELSE.
    verifySingleRewrite(
        "case when false then 1 when false then 2 else id + 1 end", nodeClass, "id + 1");
    // All FALSE, return implicit NULL ELSE.
    verifySingleRewrite("case when false then 0 end", nodeClass, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    verifyRewrite("case when id = 1 then 0 when 2 = 1 + 1 then 1 when true then 2 end",
        "CASE WHEN id = 1 THEN 0 ELSE 1 END");
    // When NULL.
    verifySingleRewrite("case when id = 0 then 0 when null then 1 else 2 end", nodeClass,
        "CASE WHEN id = 0 THEN 0 ELSE 2 END");
    // All non-constant, don't rewrite.
    verifySingleRewrite("case when id = 0 then 0 when id = 1 then 1 end", nodeClass, null);

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    verifyRewrite("case when true then 0 when false then sum(id) end", null);
    verifyRewrite(
        "case when true then count(id) when false then sum(id) end", "count(id)");
  }

  @Test
  public void testSimplifyDecode() throws ImpalaException {
    // Single TRUE case with no preceding non-constant case.
    verifyRewrite("decode(1, 0, id, 1, id + 1, 2, id + 2)", "id + 1");
    // Single TRUE case with predecing non-constant case.
    verifyRewrite("decode(1, id, id, 1, id + 1, 0)",
        "CASE WHEN id = 1 THEN id ELSE id + 1 END");
    // Single FALSE case.
    verifyRewrite("decode(1, 0, id, tinyint_col, id + 1)",
        "CASE WHEN tinyint_col = 1 THEN id + 1 END");
    // All FALSE, return ELSE.
    verifyRewrite("decode(1, 0, id, 2, 2, 3)", "3");
    // All FALSE, return implicit NULL ELSE.
    verifyRewrite("decode(1, 1 + 1, id, 1 + 2, 3)", "NULL");
    // Multiple TRUE, first one becomes ELSE.
    verifyRewrite("decode(1, id, id, 1 + 1, 0, 1 * 1, 1, 2 - 1, 2)",
        "CASE WHEN id = 1 THEN id ELSE 1 END");
    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java), so the
    // NULL case is not treated as a constant FALSE and removed.
    verifyRewrite("decode(id, null, 0, 1)", null);
    // All non-constant, don't rewrite.
    verifyRewrite("decode(id, 1, 1, 2, 2)", null);
  }

  @Test
  public void testSimplifyCoalesce() throws ImpalaException {
    Class<? extends Expr> nodeClass = FunctionCallExpr.class;

    // IMPALA-5016: Simplify COALESCE function
    // Test skipping leading nulls.
    verifySingleRewrite("coalesce(null, id, year)", nodeClass, "coalesce(id, year)");
    verifySingleRewrite("coalesce(null, 1, id)", nodeClass, "1");
    verifySingleRewrite("coalesce(null, null, id)", nodeClass, "id");
    // If the leading parameter is a non-NULL constant, rewrite to that constant.
    verifySingleRewrite("coalesce(1, id, year)", nodeClass, "1");
    // If COALESCE has only one parameter, rewrite to the parameter.
    verifySingleRewrite("coalesce(id)", nodeClass, "id");
    // If all parameters are NULL, rewrite to NULL.
    verifySingleRewrite("coalesce(null, null)", nodeClass, "NULL");
    // Do not rewrite non-literal constant exprs, rely on constant folding.
    verifySingleRewrite("coalesce(null is null, id)", nodeClass, null);
    verifySingleRewrite("coalesce(10 + null, id)", nodeClass, null);
    // Combine COALESCE nodeClass with constant folding
    verifyRewrite("coalesce(1 + 2, id, year)", "3");
    verifyRewrite("coalesce(null is null, bool_col)", "TRUE");
    verifyRewrite("coalesce(10 + null, id, year)", "coalesce(id, year)");
    // Don't rewrite based on nullability of slots. TODO (IMPALA-5753).
    verifySingleRewrite("coalesce(year, id)", nodeClass, null);
    verifySingleRewrite("functional_kudu.alltypessmall", "coalesce(id, year)", nodeClass, null);
    // IMPALA-7419: coalesce that gets simplified and contains an aggregate
    verifySingleRewrite("coalesce(null, min(distinct tinyint_col), 42)", nodeClass,
        "coalesce(min(tinyint_col), 42)");
  }

  /**
   * Special case in which a numeric literal is explicitly cast to an
   * incompatible type. In this case, no constant folding should be done.
   * Runtime relies on the fact that a DECIMAL numeric overflow in V1
   * resulted in a NULL.
   */
  @Test
  public void testCoalesceDecimal() throws ImpalaException {
    String query =
        "SELECT coalesce(1.8, CAST(0 AS DECIMAL(38,38))) AS c " +
        " FROM functional.alltypestiny";
    // Try both with and without rewrites
    for (int i = 0; i < 2; i++) {
      boolean rewrite = i == 1;

      // Analyze the expression with the rewrite option and
      // with Decimal V2 disabled.
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(rewrite);
      ctx.getQueryOptions().setDecimal_v2(false);
      SelectStmt stmt = (SelectStmt) AnalyzesOk(query, ctx);

      // Select list expr takes widest type
      Expr expr = stmt.getSelectList().getItems().get(0).getExpr();
      assertTrue(expr instanceof FunctionCallExpr);
      assertEquals(ScalarType.createDecimalType(38,38), expr.getType());

      // First arg to coalesce should be an implicit cast
      Expr arg = expr.getChild(0);
      assertTrue(arg instanceof CastExpr);
      assertEquals(ScalarType.createDecimalType(38,38), arg.getType());

      // Input to the cast is the numeric literal with its original type
      Expr num = arg.getChild(0);
      assertTrue(num instanceof NumericLiteral);
      assertEquals(ScalarType.createDecimalType(2,1), num.getType());
    }

    // In V2, the query fails with a cast exception
    try {
      AnalysisContext ctx = createAnalysisCtx();
      ctx.getQueryOptions().setEnable_expr_rewrites(true);
      ctx.getQueryOptions().setDecimal_v2(true);
      parseAndAnalyze(query, ctx);
      fail();
    } catch (SqlCastException e) {
      // Expected
    }
  }

  @Test
  public void testNormalizeExprs() throws ImpalaException {
    Class<? extends Expr> nodeClass = CompoundPredicate.class;

    verifySingleRewrite("id = 0 OR false", nodeClass, "FALSE OR id = 0");
    verifySingleRewrite("null AND true", nodeClass, "TRUE AND NULL");
    // The following already have a Boolean literal left child and don't get rewritten.
    // But, the other simplification rules do kick in.
    verifySingleRewrite("true and id = 0", nodeClass, "id = 0");
    verifySingleRewrite("false or id = 1", nodeClass, "id = 1");
    verifySingleRewrite("false or true", nodeClass, "TRUE");

    // Combine with other rules
    verifyRewrite("0 = id OR false", "id = 0");
    verifyRewrite("null AND true", "NULL");
    verifyRewrite("false or true", "TRUE");
  }

  @Test
  public void testNormalizeBinaryPredicateDeep() throws ImpalaException {
    // Verify the converse step to avoid needing to test this logic
    // in a set of queries.
    assertEquals(Operator.GT, Operator.LT.converse());
    assertEquals(Operator.GE, Operator.LE.converse());
    assertEquals(Operator.EQ, Operator.EQ.converse());
    assertEquals(Operator.NE, Operator.NE.converse());
    assertEquals(Operator.LE, Operator.GE.converse());
    assertEquals(Operator.LT, Operator.GT.converse());
    assertEquals(Operator.DISTINCT_FROM, Operator.DISTINCT_FROM.converse());
    assertEquals(Operator.NOT_DISTINCT, Operator.NOT_DISTINCT.converse());

    // Verify the details of a simple, typical rewrite case.
    // Other cases use the same code so not additional detail tests
    // are needed.
    Expr result = verifyRewrite("cast(0 as double) < id", "id > 0");

    // Verify the binary predicate
    assertTrue(result instanceof BinaryPredicate);
    assertTrue(result.isAnalyzed());
    assertEquals(Type.BOOLEAN, result.getType());
    assertEquals(3, result.getNumDistinctValues());
    assertEquals(4.0, result.getCost(), 0.001);
    // TODO: Fix this, use industry standard guess
    assertEquals(-1, result.getSelectivity(), 0.01); // Enhancement

    // The left implicit cast around id
    Expr left = result.getChild(0);
    assertTrue(left instanceof CastExpr);
    assertEquals(Type.DOUBLE, left.getType());
    assertEquals(7300, left.getNumDistinctValues());
    assertEquals(2.0, left.getCost(), 0.001);
    // TODO: Fix this, use SlotRef selectivity
    assertEquals(-1, left.getSelectivity(), 0.01); // BUG

    // The id node itself
    Expr leftValue = left.getChild(0);
    assertTrue(leftValue instanceof SlotRef);
    assertEquals(Type.INT, leftValue.getType());
    assertEquals(7300, leftValue.getNumDistinctValues());
    assertEquals(1.0, leftValue.getCost(), 0.001);
    // TODO: Fix this, use industry standard guess
    assertEquals(-1, leftValue.getSelectivity(), 0.01); // BUG: Should be 1/7300

    // The right node, 0 rewritten DOUBLE
    Expr right = result.getChild(1);
    assertTrue(right instanceof NumericLiteral);
    assertEquals(Type.DOUBLE, right.getType());
  }

  @Test
  public void testNormalizeBinaryPredicates() throws ImpalaException {
    Class<? extends Expr> nodeClass = BinaryPredicate.class;

    verifySingleRewrite("0 = id", nodeClass, "id = 0");
    verifySingleRewrite("cast(0 as double) = id", nodeClass,
        "id = CAST(0 AS DOUBLE)");
    verifySingleRewrite("1 + 1 = cast(id as int)", nodeClass,
        "CAST(id AS INT) = 1 + 1");
    verifySingleRewrite("5 = id + 2", nodeClass, "id + 2 = 5");
    verifySingleRewrite("5 + 3 = id", nodeClass, "id = 5 + 3");
    verifySingleRewrite("tinyint_col + smallint_col = int_col",
        nodeClass, "int_col = tinyint_col + smallint_col");
    // Two levels of cast
    verifySingleRewrite("cast(cast(0 as smallint) as double) < cast(cast(id as int) as double)",
        nodeClass, "CAST(CAST(id AS INT) AS DOUBLE) > CAST(CAST(0 AS SMALLINT) AS DOUBLE)");
    // Col ref is inside an expression
    verifySingleRewrite("5 + 3 = id + 2", nodeClass, "id + 2 = 5 + 3");
    // Bare slot ref goes on left, even if other side is a slot ref expression
    verifySingleRewrite("id + 6 = int_col", nodeClass, "int_col = id + 6");
    verifySingleRewrite("sum(id) = sum(int_col) + 1", nodeClass, null);
    verifySingleRewrite("sum(int_col) + 1 = sum(id)", nodeClass, null);

    // Verify that these don't get rewritten.
    verifySingleRewrite("5 = 6", nodeClass, null);
    verifySingleRewrite("id = 5", nodeClass, null);
    verifySingleRewrite("cast(id as int) = int_col", nodeClass, null);
    verifySingleRewrite("int_col = cast(id as int)", nodeClass, null);
    verifySingleRewrite("int_col = tinyint_col", nodeClass, null);
    verifySingleRewrite("tinyint_col = int_col", nodeClass, null);
    verifySingleRewrite("2 + 6 = 8", nodeClass, null);
    verifySingleRewrite("8 = 2 + 6", nodeClass, null);
    verifySingleRewrite("int_col = id + 6", nodeClass, null);

    // Full rewrite tests
    verifyRewrite("0 = id", "id = 0");
    verifyRewrite("cast(0 as double) = id", "id = 0");
    verifyRewrite("1 + 1 = cast(id as int)", "CAST(id AS INT) = 2");
    verifyRewrite("5 = id + 2", "id + 2 = 5");
    verifyRewrite("5 + 3 = id", "id = 8");
    verifyRewrite("tinyint_col + smallint_col = int_col",
        "int_col = tinyint_col + smallint_col");
    // Two levels of cast
    verifyRewrite("cast(cast(0 as smallint) as double) < cast(cast(id as int) as double)",
        "CAST(CAST(id AS INT) AS DOUBLE) > 0");
    // Col ref is inside an expression
    verifyRewrite("5 + 3 = id + 2", "id + 2 = 8");
    // Bare slot ref goes on left, even if other side is a slot ref expression
    verifyRewrite("id + 6 = int_col", "int_col = id + 6");
    verifyRewrite("sum(id) = sum(int_col) + 1", null);
    verifyRewrite("sum(int_col) + 1 = sum(id)", null);

    // Verify that these don't get rewritten.
    verifyRewrite("5 = 6", "FALSE");
    verifyRewrite("id = 5", null);
    verifyRewrite("cast(id as int) = int_col", null);
    verifyRewrite("int_col = cast(id as int)", null);
    verifyRewrite("int_col = tinyint_col", null);
    verifyRewrite("tinyint_col = int_col", null);
    verifyRewrite("2 + 6 = 8", "TRUE");
    verifyRewrite("8 = 2 + 6", "TRUE");
    verifyRewrite("int_col = id + 6", null);
  }

  @Test
  public void testEqualityDisjunctsToIn() throws ImpalaException {
    Class<? extends Expr> nodeClass = CompoundPredicate.class;

    verifySingleRewrite("int_col = 1 or int_col = 2", nodeClass, "int_col IN (1, 2)");
    verifyRewrite("int_col = 1 or int_col = 2 or int_col = 3",
        "int_col IN (1, 2, 3)");
    verifyRewrite("(int_col = 1 or int_col = 2) or (int_col = 3 or int_col = 4)",
        "int_col IN (1, 2, 3, 4)");
    verifyRewrite("float_col = 1.1 or float_col = 2.2 or float_col = 3.3",
        "float_col IN (1.1, 2.2, 3.3)");
    verifyRewrite("string_col = '1' or string_col = '2' or string_col = '3'",
        "string_col IN ('1', '2', '3')");
    verifyRewrite("bool_col = true or bool_col = false or bool_col = true",
        "bool_col IN (TRUE, FALSE, TRUE)");
    verifyRewrite("bool_col = null or bool_col = null or bool_col is null",
        "bool_col = NULL OR bool_col IS NULL");
    verifyRewrite("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 = 12",
        "int_col * 3 IN (6, 9, 12)");

    // cases where rewrite should happen partially
    // Note: the binary predicate rewrite ensures proper form of each predicate
    verifyRewrite("(int_col = 1 or int_col = 2) or (int_col = 3 and int_col = 4)",
        "int_col IN (1, 2) OR (int_col = 3 AND int_col = 4)");
    verifyRewrite(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        "int_col IN (1, 2) OR int_col = 3 AND float_col IN (5, 6)");
    verifyRewrite(
        "(1 = int_col or 2 = int_col or 3 = int_col) AND (float_col = 5 or float_col = 6)",
        "int_col IN (1, 2, 3) AND float_col IN (5, 6)");
    verifyRewrite("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 <= 12",
        "int_col * 3 IN (6, 9) OR int_col * 3 <= 12");

    // combo rules
    verifyRewrite(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        "int_col IN (1, 2) OR int_col = 3 AND float_col IN (5, 6)");

    // existing in predicate
    verifySingleRewrite("int_col in (1,2) or int_col = 3", nodeClass,
        "int_col IN (1, 2, 3)");
    verifySingleRewrite("int_col = 1 or int_col in (2, 3)", nodeClass,
        "int_col IN (2, 3, 1)");
    verifySingleRewrite("int_col in (1, 2) or int_col in (3, 4)", nodeClass,
        "int_col IN (1, 2, 3, 4)");

    // no rewrite
    verifySingleRewrite("int_col = smallint_col or int_col = bigint_col ", nodeClass, null);
    verifySingleRewrite("int_col = 1 or int_col = int_col ", nodeClass, null);
    verifySingleRewrite("int_col = 1 or int_col = int_col + 3 ", nodeClass, null);
    verifySingleRewrite("int_col in (1, 2) or int_col = int_col + 3 ", nodeClass, null);
    verifySingleRewrite("int_col not in (1,2) or int_col = 3", nodeClass, null);
    verifySingleRewrite("int_col = 3 or int_col not in (1,2)", nodeClass, null);
    verifySingleRewrite("int_col not in (1,2) or int_col not in (3, 4)", nodeClass, null);
    verifySingleRewrite("int_col in (1,2) or int_col not in (3, 4)", nodeClass, null);

    // TODO if subqueries are supported in OR clause in future, add tests to cover the same.
    // TODO Fix the following
//    verifyWhereRewrite(
//        "int_col = 1 and int_col in "
//            + "(select smallint_col from functional.alltypessmall where smallint_col<10)",
//        null);
  }

  @Test
  public void testNormalizeCountStar() throws ImpalaException {
    Class<? extends Expr> nodeClass = FunctionCallExpr.class;

    verifySingleRewrite("count(1)", nodeClass, "count(*)");
    verifySingleRewrite("count(5)", nodeClass, "count(*)");

    // Verify that these don't get rewritten.
    verifySingleRewrite("count(null)", nodeClass, null);
    verifySingleRewrite("count(id)", nodeClass, null);
    verifySingleRewrite("count(1 + 1)", nodeClass, null);
    verifySingleRewrite("count(1 + null)", nodeClass, null);

    // In conjunction with other function rewrites
    verifyRewrite("if(true, count(1), 0)", "count(*)");
  }

  @Test
  public void testSimplifyDistinctFrom() throws ImpalaException {
    Class<? extends Expr> nodeClass = BinaryPredicate.class;

    // Can be simplified
    verifySingleRewrite("bool_col IS DISTINCT FROM bool_col", nodeClass, "FALSE");
    verifySingleRewrite("bool_col IS NOT DISTINCT FROM bool_col", nodeClass, "TRUE");
    verifySingleRewrite("bool_col <=> bool_col", nodeClass, "TRUE");

    // Verify nothing happens
    verifySingleRewrite("bool_col IS NOT DISTINCT FROM int_col", nodeClass, null);
    verifySingleRewrite("bool_col IS DISTINCT FROM int_col", nodeClass, null);

    // IF with distinct and distinct from
    verifyRewrite("if(bool_col is distinct from bool_col, 1, 2)", "2");
    verifyRewrite("if(bool_col is not distinct from bool_col, 1, 2)", "1");
    verifyRewrite("if(bool_col <=> bool_col, 1, 2)", "1");
    verifyRewrite("if(bool_col <=> NULL, 1, 2)", null);
  }

  /**
   * NULLIF gets converted to an IF, and has cases where
   * it can be further simplified via SimplifyDistinctFromRule.
   */
  @Test
  public void testNullif() throws ImpalaException {
    // nullif: converted to if and simplified
    verifyRewrite("nullif(bool_col, bool_col)", "NULL");

    // works because the expression tree is identical;
    // more complicated things like nullif(int_col + 1, 1 + int_col)
    // are not simplified
    verifyRewrite("nullif(1 + int_col, 1 + int_col)", "NULL");
  }
}
