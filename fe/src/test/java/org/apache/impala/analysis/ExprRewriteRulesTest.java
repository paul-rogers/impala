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

import java.util.List;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.ExprAnalyzer.RewriteMode;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeCountStarRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.apache.impala.rewrite.RemoveRedundantStringCast;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.SimplifyDistinctFromRule;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests ExprRewriteRules.
 */
public class ExprRewriteRulesTest extends FrontendTestBase {

  /** Wraps an ExprRewriteRule to count how many times it's been applied. */
  private static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    int rewrites;
    ExprRewriteRule wrapped;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      Expr ret = wrapped.apply(expr, analyzer);
      if (expr != ret) rewrites++;
      return ret;
    }
  }

  public Expr RewritesOk(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOk(String exprStr, List<ExprRewriteRule> rules, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rules, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select " + exprStr + " from " + tableName;
    // Analyze without rewrites since that's what we want to test here.
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    AnalyzesOkNoRewrite(stmt);
    Expr origExpr = stmt.getSelectList().getItems().get(0).getExpr();
    Expr rewrittenExpr =
        verifyExprEquivalence(origExpr, expectedExprStr, rules, stmt.getAnalyzer());
    return rewrittenExpr;
  }

  private Expr verifyLegacyRewrite(String exprSql, String expectedSql) {
    String stmtSql = "select " + exprSql + " from functional.alltypessmall";
    SelectStmt stmt = (SelectStmt) AnalyzesOk(stmtSql);
    Expr expr = stmt.getSelectList().getItems().get(0).getExpr();
    assertEquals(expectedSql == null ? exprSql : expectedSql, expr.toSql());
    return expr;
  }

  public Expr RewritesOkWhereExpr(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOkWhereExpr("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String tableName, String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOkWhereExpr(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOkWhereExpr(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    String stmtStr = "select count(1)  from " + tableName + " where " + exprStr;
    // Analyze without rewrites since that's what we want to test here.
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    AnalyzesOkNoRewrite(stmt);
    Expr origExpr = stmt.getWhereClause();
    Expr rewrittenExpr =
        verifyExprEquivalence(origExpr, expectedExprStr, rules, stmt.getAnalyzer());
    return rewrittenExpr;
  }

  private Expr verifyExprEquivalence(Expr origExpr, String expectedExprStr,
      List<ExprRewriteRule> rules, Analyzer analyzer) throws AnalysisException {
    String origSql = origExpr.toSql();

    List<ExprRewriteRule> wrappedRules = Lists.newArrayList();
    for (ExprRewriteRule r : rules) {
      wrappedRules.add(new CountingRewriteRuleWrapper(r));
    }
    ExprRewriter rewriter = new ExprRewriter(wrappedRules);

    Expr rewrittenExpr = rewriter.rewrite(origExpr, analyzer);
    String rewrittenSql = rewrittenExpr.toSql();
    boolean expectChange = expectedExprStr != null;
    if (expectedExprStr != null) {
      assertEquals(expectedExprStr, rewrittenSql);
      // Asserts that all specified rules fired at least once. This makes sure that the
      // rules being tested are, in fact, being executed. A common mistake is to write
      // an expression that's re-written by the constant folder before getting to the
      // rule that is intended for the test.
      for (ExprRewriteRule r : wrappedRules) {
        CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
        Assert.assertTrue("Rule " + w.wrapped.toString() + " didn't fire.",
          w.rewrites > 0);
      }
    } else {
      assertEquals(origSql, rewrittenSql);
    }
    Assert.assertEquals(expectChange, rewriter.changed());
    return rewrittenExpr;
  }

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

  private Expr parseSelectExpr(String exprSql) {
    String stmtSql = "select " + exprSql + " from functional.alltypessmall";
    return ((SelectStmt) ParsesOk(stmtSql)).getSelectList().getItems().get(0).getExpr();
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
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
    return result;
  }

  private void verifySingleRewrite(String exprSql, Class<? extends Expr> nodeClass,
      String expectedSql) throws ImpalaException {
    Expr expr = analyzeWithoutRewrite(exprSql);
    assertTrue(nodeClass.isInstance(expr));
    Expr result = expr.rewrite(RewriteMode.OPTIONAL);
    if (result == expr) {
      assertNull("Expected rewrite", expectedSql);
    } else {
      assertNotNull("Expected no rewrite", expectedSql);
      assertEquals(expectedSql, result.toSql());
    }
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
    ExprRewriteRule rule = ExtractCommonConjunctRule.INSTANCE;

    // One common conjunct: int_col < 10
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10)", rule,
        "int_col < 10 AND ((bigint_col < 10) OR (string_col = '10'))");
    // One common conjunct in multiple disjuncts: int_col < 10
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and float_col > 3.14)", rule,
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Same as above but with a bushy OR tree.
    RewritesOk(
        "((int_col < 10 and bigint_col < 10) or " +
        " (string_col = '10' and int_col < 10)) or " +
        "((id < 20 and int_col < 10) or " +
        " (int_col < 10 and float_col > 3.14))", rule,
        "int_col < 10 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Multiple common conjuncts: int_col < 10, bool_col is null
    RewritesOk(
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(bool_col is null and string_col = '10' and int_col < 10)", rule,
        "int_col < 10 AND bool_col IS NULL AND " +
        "((bigint_col < 10) OR (string_col = '10'))");
    // Negated common conjunct: !(int_col=5 or tinyint_col > 9)
    RewritesOk(
        "(!(int_col=5 or tinyint_col > 9) and double_col = 7) or " +
        "(!(int_col=5 or tinyint_col > 9) and double_col = 8)", rule,
        "NOT (int_col = 5 OR tinyint_col > 9) AND " +
        "((double_col = 7) OR (double_col = 8))");

    // Test common BetweenPredicate: int_col between 10 and 30
    RewritesOk(
        "(int_col between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)", rule,
        "int_col >= 10 AND int_col <= 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test common NOT BetweenPredicate: int_col not between 10 and 30
    RewritesOk(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col not between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col not between 10 and 30 and float_col > 3.14)", rule,
        "int_col < 10 OR int_col > 30 AND " +
        "((bigint_col < 10) OR (string_col = '10') OR " +
        "(id < 20) OR (float_col > 3.14))");
    // Test mixed BetweenPredicates are not common.
    RewritesOk(
        "(int_col not between 10 and 30 and bigint_col < 10) or " +
        "(string_col = '10' and int_col between 10 and 30) or " +
        "(id < 20 and int_col not between 10 and 30) or " +
        "(int_col between 10 and 30 and float_col > 3.14)", rule,
        null);

    // All conjuncts are common.
    RewritesOk(
        "(int_col < 10 and id between 5 and 6) or " +
        "(id between 5 and 6 and int_col < 10) or " +
        "(int_col < 10 and id between 5 and 6)", rule,
        "int_col < 10 AND id >= 5 AND id <= 6");
    // Complex disjuncts are redundant.
    RewritesOk(
        "(int_col < 10) or " +
        "(int_col < 10 and bigint_col < 10 and bool_col is null) or " +
        "(int_col < 10) or " +
        "(bool_col is null and int_col < 10)", rule,
        "int_col < 10");

    // Due to the shape of the original OR tree we are left with redundant
    // disjuncts after the extraction.
    RewritesOk(
        "(int_col < 10 and bigint_col < 10) or " +
        "(string_col = '10' and int_col < 10) or " +
        "(id < 20 and int_col < 10) or " +
        "(int_col < 10 and id < 20)", rule,
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
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
    // CASE with caseExpr
    // Single TRUE case with no preceding non-constant cases.
    RewritesOk("case 1 when 0 then id when 1 then id + 1 when 2 then id + 2 end", rule,
        "id + 1");
    // SINGLE TRUE case with preceding non-constant case.
    RewritesOk("case 1 when id then id when 1 then id + 1 end", rule,
        "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("case 0 when 1 then 1 when id then id + 1 end", rule,
        "CASE 0 WHEN id THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("case 2 when 0 then id when 1 then id * 2 else 0 end", rule, "0");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case 3 when 0 then id when 1 then id + 1 end", rule, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case 1 when id then id when 2 - 1 then id + 1 when 1 then id + 2 end",
        rules, "CASE 1 WHEN id THEN id ELSE id + 1 END");
    // When NULL.
    RewritesOk("case 0 when null then id else 1 end", rule, "1");
    // All non-constant, don't rewrite.
    RewritesOk("case id when 1 then 1 when 2 then 2 else 3 end", rule, null);
  }

  @Test
  public void testSimplifyCaseWithoutValue() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // CASE without caseExpr
    // Single TRUE case with no predecing non-constant case.
    RewritesOk("case when FALSE then 0 when TRUE then 1 end", rule, "1");
    // Single TRUE case with preceding non-constant case.
    RewritesOk("case when id = 0 then 0 when true then 1 when id = 2 then 2 end", rule,
        "CASE WHEN id = 0 THEN 0 ELSE 1 END");
    // Single FALSE case.
    RewritesOk("case when id = 0 then 0 when false then 1 when id = 2 then 2 end", rule,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");
    // All FALSE, return ELSE.
    RewritesOk(
        "case when false then 1 when false then 2 else id + 1 end", rule, "id + 1");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("case when false then 0 end", rule, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("case when id = 1 then 0 when 2 = 1 + 1 then 1 when true then 2 end",
        rules, "CASE WHEN id = 1 THEN 0 ELSE 1 END");
    // When NULL.
    RewritesOk("case when id = 0 then 0 when null then 1 else 2 end", rule,
        "CASE WHEN id = 0 THEN 0 ELSE 2 END");
    // All non-constant, don't rewrite.
    RewritesOk("case when id = 0 then 0 when id = 1 then 1 end", rule, null);

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    RewritesOk("case when true then 0 when false then sum(id) end", rule, null);
    RewritesOk(
        "case when true then count(id) when false then sum(id) end", rule, "count(id)");
  }

  @Test
  public void testSimplifyDecode() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // DECODE
    // Single TRUE case with no preceding non-constant case.
    RewritesOk("decode(1, 0, id, 1, id + 1, 2, id + 2)", rules, "id + 1");
    // Single TRUE case with predecing non-constant case.
    RewritesOk("decode(1, id, id, 1, id + 1, 0)", rules,
        "CASE WHEN id = 1 THEN id ELSE id + 1 END");
    // Single FALSE case.
    RewritesOk("decode(1, 0, id, tinyint_col, id + 1)", rules,
        "CASE WHEN tinyint_col = 1 THEN id + 1 END");
    // All FALSE, return ELSE.
    RewritesOk("decode(1, 0, id, 2, 2, 3)", rules, "3");
    // All FALSE, return implicit NULL ELSE.
    RewritesOk("decode(1, 1 + 1, id, 1 + 2, 3)", rules, "NULL");
    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("decode(1, id, id, 1 + 1, 0, 1 * 1, 1, 2 - 1, 2)", rules,
        "CASE WHEN id = 1 THEN id ELSE 1 END");
    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java), so the
    // NULL case is not treated as a constant FALSE and removed.
    RewritesOk("decode(id, null, 0, 1)", rules, null);
    // All non-constant, don't rewrite.
    RewritesOk("decode(id, 1, 1, 2, 2)", rules, null);
  }

  @Test
  public void testSimplifyCoalesce() throws ImpalaException {
    ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // IMPALA-5016: Simplify COALESCE function
    // Test skipping leading nulls.
    RewritesOk("coalesce(null, id, year)", rule, "coalesce(id, year)");
    RewritesOk("coalesce(null, 1, id)", rule, "1");
    RewritesOk("coalesce(null, null, id)", rule, "id");
    // If the leading parameter is a non-NULL constant, rewrite to that constant.
    RewritesOk("coalesce(1, id, year)", rule, "1");
    // If COALESCE has only one parameter, rewrite to the parameter.
    RewritesOk("coalesce(id)", rule, "id");
    // If all parameters are NULL, rewrite to NULL.
    RewritesOk("coalesce(null, null)", rule, "NULL");
    // Do not rewrite non-literal constant exprs, rely on constant folding.
    RewritesOk("coalesce(null is null, id)", rule, null);
    RewritesOk("coalesce(10 + null, id)", rule, null);
    // Combine COALESCE rule with FoldConstantsRule.
    RewritesOk("coalesce(1 + 2, id, year)", rules, "3");
    RewritesOk("coalesce(null is null, bool_col)", rules, "TRUE");
    RewritesOk("coalesce(10 + null, id, year)", rules, "coalesce(id, year)");
    // Don't rewrite based on nullability of slots. TODO (IMPALA-5753).
    RewritesOk("coalesce(year, id)", rule, null);
    RewritesOk("functional_kudu.alltypessmall", "coalesce(id, year)", rule, null);
    // IMPALA-7419: coalesce that gets simplified and contains an aggregate
    RewritesOk("coalesce(null, min(distinct tinyint_col), 42)", rule,
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
    verifySingleRewrite("id = 0 OR false", CompoundPredicate.class, "FALSE OR id = 0");
    verifySingleRewrite("null AND true", CompoundPredicate.class, "TRUE AND NULL");
    // The following already have a Boolean literal left child and don't get rewritten.
    verifySingleRewrite("true and id = 0", CompoundPredicate.class, null);
    verifySingleRewrite("false or id = 1", CompoundPredicate.class, null);
    verifySingleRewrite("false or true", CompoundPredicate.class, null);

    // Combine with other rules
    verifyRewrite("0 = id OR false", "FALSE OR id = 0");
    verifyRewrite("null AND true", "NULL");
    verifyRewrite("true and id = 0", null);
    verifyRewrite("false or id = 1", null);
    verifyRewrite("false or true", "TRUE");
  }

  private Expr rewriteTopOnly(String exprSql) throws ImpalaException {
    Expr expr = parseSelectExpr(exprSql);
    assertTrue(expr instanceof BinaryPredicate);

    Analyzer analyzer = prepareAnalyzer(false);
    Expr result = analyzer.analyzeAndRewrite(expr);
    result = result.rewrite(RewriteMode.OPTIONAL);
    analyzer.analyzeInPlace(result);
    return result;
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
    Expr result = rewriteTopOnly("cast(0 as double) < id");
    assertEquals("id > CAST(0 AS DOUBLE)", result.toSql());

    // Verify the binary predicate
    assertTrue(result instanceof BinaryPredicate);
    assertTrue(result.isAnalyzed());
    assertEquals(Type.BOOLEAN, result.getType());
    assertEquals(3, result.getNumDistinctValues());
    assertEquals(5.0, result.getCost(), 0.001);
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

    // The right node, the explicit cast to DOUBLE
    Expr right = result.getChild(1);
    assertTrue(right instanceof CastExpr);
    assertEquals(Type.DOUBLE, right.getType());
    assertEquals(1, right.getNumDistinctValues());
    assertEquals(2.0, right.getCost(), 0.001);
    // TODO: Fix this, use indistry standard guess
    assertEquals(-1, right.getSelectivity(), 0.01);

    // The child should be a numeric literal
    Expr rightValue = right.getChild(0);
    assertTrue(rightValue instanceof NumericLiteral);
    assertEquals(Type.DOUBLE, rightValue.getType()); // Bug: should be TINYINT
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
  public void TestEqualityDisjunctsToInRule() throws ImpalaException {
    ExprRewriteRule edToInrule = EqualityDisjunctsToInRule.INSTANCE;
    List<ExprRewriteRule> comboRules = Lists.newArrayList(edToInrule);

    RewritesOk("int_col = 1 or int_col = 2", edToInrule, "int_col IN (1, 2)");
    RewritesOk("int_col = 1 or int_col = 2 or int_col = 3", edToInrule,
        "int_col IN (1, 2, 3)");
    RewritesOk("(int_col = 1 or int_col = 2) or (int_col = 3 or int_col = 4)", edToInrule,
        "int_col IN (1, 2, 3, 4)");
    RewritesOk("float_col = 1.1 or float_col = 2.2 or float_col = 3.3",
        edToInrule, "float_col IN (1.1, 2.2, 3.3)");
    RewritesOk("string_col = '1' or string_col = '2' or string_col = '3'",
        edToInrule, "string_col IN ('1', '2', '3')");
    RewritesOk("bool_col = true or bool_col = false or bool_col = true", edToInrule,
        "bool_col IN (TRUE, FALSE, TRUE)");
    RewritesOk("bool_col = null or bool_col = null or bool_col is null", edToInrule,
        "bool_col IN (NULL, NULL) OR bool_col IS NULL");
    RewritesOk("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 = 12",
        edToInrule, "int_col * 3 IN (6, 9, 12)");

    // cases where rewrite should happen partially
    // Note: the binary predicate rewrite ensures proper form of each predicate
    RewritesOk("(int_col = 1 or int_col = 2) or (int_col = 3 and int_col = 4)",
        edToInrule, "int_col IN (1, 2) OR (int_col = 3 AND int_col = 4)");
    RewritesOk(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        edToInrule,
        "int_col IN (1, 2) OR int_col = 3 AND float_col IN (5, 6)");
    RewritesOk(
        "(1 = int_col or 2 = int_col or 3 = int_col) AND (float_col = 5 or float_col = 6)",
        edToInrule,
        "int_col IN (1, 2, 3) AND float_col IN (5, 6)");
    RewritesOk("int_col * 3 = 6 or int_col * 3 = 9 or int_col * 3 <= 12",
        edToInrule, "int_col * 3 IN (6, 9) OR int_col * 3 <= 12");

    // combo rules
    RewritesOk(
        "1 = int_col or 2 = int_col or 3 = int_col AND (float_col = 5 or float_col = 6)",
        comboRules, "int_col IN (1, 2) OR int_col = 3 AND float_col IN (5, 6)");

    // existing in predicate
    RewritesOk("int_col in (1,2) or int_col = 3", edToInrule,
        "int_col IN (1, 2, 3)");
    RewritesOk("int_col = 1 or int_col in (2, 3)", edToInrule,
        "int_col IN (2, 3, 1)");
    RewritesOk("int_col in (1, 2) or int_col in (3, 4)", edToInrule,
        "int_col IN (1, 2, 3, 4)");

    // no rewrite
    RewritesOk("int_col = smallint_col or int_col = bigint_col ", edToInrule, null);
    RewritesOk("int_col = 1 or int_col = int_col ", edToInrule, null);
    RewritesOk("int_col = 1 or int_col = int_col + 3 ", edToInrule, null);
    RewritesOk("int_col in (1, 2) or int_col = int_col + 3 ", edToInrule, null);
    RewritesOk("int_col not in (1,2) or int_col = 3", edToInrule, null);
    RewritesOk("int_col = 3 or int_col not in (1,2)", edToInrule, null);
    RewritesOk("int_col not in (1,2) or int_col not in (3, 4)", edToInrule, null);
    RewritesOk("int_col in (1,2) or int_col not in (3, 4)", edToInrule, null);

    // TODO if subqueries are supported in OR clause in future, add tests to cover the same.
    RewritesOkWhereExpr(
        "int_col = 1 and int_col in "
            + "(select smallint_col from functional.alltypessmall where smallint_col<10)",
        edToInrule, null);
  }

  @Test
  public void TestNormalizeCountStarRule() throws ImpalaException {
    ExprRewriteRule rule = NormalizeCountStarRule.INSTANCE;

    RewritesOk("count(1)", rule, "count(*)");
    RewritesOk("count(5)", rule, "count(*)");

    // Verify that these don't get rewritten.
    RewritesOk("count(null)", rule, null);
    RewritesOk("count(id)", rule, null);
    RewritesOk("count(1 + 1)", rule, null);
    RewritesOk("count(1 + null)", rule, null);
  }

  @Test
  public void TestSimplifyDistinctFromRule() throws ImpalaException {
    ExprRewriteRule rule = SimplifyDistinctFromRule.INSTANCE;

    // Can be simplified
    RewritesOk("bool_col IS DISTINCT FROM bool_col", rule, "FALSE");
    RewritesOk("bool_col IS NOT DISTINCT FROM bool_col", rule, "TRUE");
    RewritesOk("bool_col <=> bool_col", rule, "TRUE");

    // Verify nothing happens
    RewritesOk("bool_col IS NOT DISTINCT FROM int_col", rule, null);
    RewritesOk("bool_col IS DISTINCT FROM int_col", rule, null);

    // IF with distinct and distinct from
    List<ExprRewriteRule> rules = Lists.newArrayList(
        SimplifyConditionalsRule.INSTANCE,
        SimplifyDistinctFromRule.INSTANCE);
    RewritesOk("if(bool_col is distinct from bool_col, 1, 2)", rules, "2");
    RewritesOk("if(bool_col is not distinct from bool_col, 1, 2)", rules, "1");
    RewritesOk("if(bool_col <=> bool_col, 1, 2)", rules, "1");
    RewritesOk("if(bool_col <=> NULL, 1, 2)", rules, null);
  }

  @Test
  public void TestRemoveRedundantStringCastRule() throws ImpalaException {
    ExprRewriteRule removeRule = RemoveRedundantStringCast.INSTANCE;
    ExprRewriteRule foldConstantRule = FoldConstantsRule.INSTANCE;
    List<ExprRewriteRule> comboRules = Lists.newArrayList(removeRule, foldConstantRule);

    // Can be simplified.
    RewritesOk("cast(tinyint_col as string) = '100'", comboRules, "tinyint_col = 100");
    RewritesOk("cast(smallint_col as string) = '1000'", comboRules,
        "smallint_col = 1000");
    RewritesOk("cast(int_col as string) = '123456'", comboRules, "int_col = 123456");
    RewritesOk("cast(bigint_col as string) = '9223372036854775807'", comboRules,
        "bigint_col = 9223372036854775807");
    RewritesOk("cast(float_col as string) = '1000.5'", comboRules, "float_col = 1000.5");
    RewritesOk("cast(double_col as string) = '1000.5'", comboRules,
        "double_col = 1000.5");
    RewritesOk("cast(timestamp_col as string) = '2009-01-01 00:01:00'", comboRules,
        "timestamp_col = TIMESTAMP '2009-01-01 00:01:00'");
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '2.2222'", comboRules,
        "c1 = 2.2222");

    RewritesOk("cast(tinyint_col as string) = '-100'", comboRules, "tinyint_col = -100");
    RewritesOk("cast(smallint_col as string) = '-1000'", comboRules,
        "smallint_col = -1000");
    RewritesOk("cast(int_col as string) = '-123456'", comboRules, "int_col = -123456");
    RewritesOk("cast(bigint_col as string) = '-9223372036854775807'", comboRules,
        "bigint_col = -9223372036854775807");
    RewritesOk("cast(float_col as string) = '-1000.5'", comboRules,
        "float_col = -1000.5");
    RewritesOk("cast(double_col as string) = '-1000.5'", comboRules,
        "double_col = -1000.5");
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '-2.2222'", comboRules,
        "c1 = -2.2222");

    // Works for VARCHAR/CHAR.
    RewritesOk("cast(tinyint_col as char(3)) = '100'", comboRules, "tinyint_col = 100");
    RewritesOk("cast(tinyint_col as varchar(3)) = '100'", comboRules,
        "tinyint_col = 100");
    RewritesOk("functional.chars_tiny", "cast(cs as string) = 'abc'",
        comboRules, "cs = 'abc  '"); // column 'cs' is char(5), hence the trailing spaces.
    RewritesOk("functional.chars_tiny", "cast(vc as string) = 'abc'",
        comboRules, "vc = 'abc'");

    // Works with complex expressions on both sides.
    RewritesOk("cast(cast(int_col + 1 as double) as string) = '123456'", comboRules,
        "CAST(int_col + 1 AS DOUBLE) = 123456");
    RewritesOk("cast(int_col + 1 as string) = concat('123', '456')", comboRules,
        "int_col + 1 = 123456");
    RewritesOk("cast(int_col as string) = ltrim(concat('     123', '456'))", comboRules,
        "int_col = 123456");
    RewritesOk("cast(int_col as string) = strleft('123456789', 6)", comboRules,
        "int_col = 123456");
    RewritesOk("cast(tinyint_col as char(3)) = cast(100 as char(3))", comboRules,
        "tinyint_col = 100");
    RewritesOk("cast(tinyint_col as char(3)) = cast(100 as varchar(3))", comboRules,
        "tinyint_col = 100");

    // Verify nothing happens.
    RewritesOk("cast(tinyint_col as string) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as string) = '01000'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = '01000'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = '1000000000'", comboRules, null);
    RewritesOk("cast(int_col as string) = '02147483647'", comboRules, null);
    RewritesOk("cast(int_col as string) = '21474836470'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = '09223372036854775807'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = '92233720368547758070'", comboRules, null);
    RewritesOk("cast(float_col as string) = '01000.5'", comboRules, null);
    RewritesOk("cast(double_col as string) = '01000.5'", comboRules, null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '02.2222'", comboRules,
        null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = '2.22'",
        comboRules, null);
    RewritesOk("cast(timestamp_col as string) = '2009-15-01 00:01:00'", comboRules, null);
    RewritesOk("cast(tinyint_col as char(4)) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as varchar(4)) = '0100'", comboRules, null);
    RewritesOk("cast(tinyint_col as char(2)) = '100'", comboRules, null);
    RewritesOk("cast(tinyint_col as varchar(2)) = '100'", comboRules, null);

    // 'NULL' is treated like any other string, so no conversion should take place.
    RewritesOk("cast(tinyint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(smallint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(int_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(bigint_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(float_col as string) = 'NULL'", comboRules, null);
    RewritesOk("cast(double_col as string) = 'NULL'", comboRules, null);
    RewritesOk("functional.decimal_tiny", "cast(c1 as string) = 'NULL'",
        comboRules, null);
    RewritesOk("cast(timestamp_col as string) = 'NULL'", comboRules, null);
  }

  /**
   * NULLIF gets converted to an IF, and has cases where
   * it can be further simplified via SimplifyDistinctFromRule.
   */
  @Test
  public void TestNullif() throws ImpalaException {
    List<ExprRewriteRule> rules = Lists.newArrayList(
        SimplifyConditionalsRule.INSTANCE,
        SimplifyDistinctFromRule.INSTANCE);

    // nullif: converted to if and simplified
    RewritesOk("nullif(bool_col, bool_col)", rules, "NULL");

    // works because the expression tree is identical;
    // more complicated things like nullif(int_col + 1, 1 + int_col)
    // are not simplified
    RewritesOk("nullif(1 + int_col, 1 + int_col)", rules, "NULL");
  }
}
