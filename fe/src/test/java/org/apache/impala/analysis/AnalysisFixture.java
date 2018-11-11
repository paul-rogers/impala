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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.EventSequence;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Test fixture to analyze queries using a defined set of
 * parameters and options. Provides access to the intermediate
 * state created during analysis to allow finer-grain probing
 * during tests.
 *
 * The AnalysisFixture holds overall state: the frontend,
 * user, database and query options.
 *
 * The QueryFixture holds per-query state which can include a test-specific
 * set of options. The query can be analyzed in full, or only analyzed up
 * to rewrites. It provides access to normally-hidden temporary objects
 * such as the analyzer, analysis context and so on. Tests can probe
 * these objects to check for conditions of interest.
 *
 * The Rewrite fixture builds on the QueryFixture to add additional
 * tools needed to probe rewrites of the various clauses within a
 * query.
 *
 * Unlike the functions in FrontEndTestBase, this fixture does
 * not set the option to skip rewrites. Rather, each test can
 * elect to disable them (for testing individual rules) or to
 * leave them enabled (for tests of the overall rewrite process.)
 */
public class AnalysisFixture {

  /**
   * Performs analysis of a query and provides access to the
   * internal details to allow probing of this details during
   * testing.
   */
  public class QueryFixture {

    private final TQueryCtx queryCtx;
    private final String stmt;
    private ParseNode parseResult;
    private AnalysisContext ctx;
    private StatementBase parsedStmt;
    private AnalysisResult analysisResult;
    private Analyzer analyzer;

    public QueryFixture(TQueryCtx queryCtx, String stmt) {
      this.queryCtx = queryCtx;
      this.stmt = stmt;
    }

    /**
     * Low-level parse.
     */
    public ParseNode parse() {
      SqlScanner input = new SqlScanner(new StringReader(stmt));
      SqlParser parser = new SqlParser(input);
      parser.setQueryOptions(queryOptions);
      try {
        parseResult = (ParseNode) parser.parse().value;
      } catch (Exception e) {
        e.printStackTrace();
        fail("\nParser error:\n" + parser.getErrorMsg(stmt));
      }
      assertNotNull(parseResult);
      return parseResult;
    }

    /**
     * Full analysis, including expression rewrites.
     */
    public QueryFixture analyze() throws AnalysisException {
      makeAnalysisContext();

      try {
        // Parse the statement
        parsedStmt = frontend.parse(stmt, ctx.getQueryOptions());

        analysisResult = ctx.analyzeAndAuthorize(parsedStmt,
            makeTableCache(), frontend.getAuthzChecker());
        Preconditions.checkNotNull(analysisResult.getStmt());
        analyzer = analysisResult.getAnalyzer();
      } catch (AnalysisException e) {
        throw e;
      } catch (ImpalaException e) {
        fail(e.getMessage());
        // To keep the Java parser happy.
        throw new IllegalStateException(e);
      }
      return this;
    }

    /**
     * Partial analysis that excludes expression rewrites.
     */
    public QueryFixture analyzeWithoutRewrite() {
      makeAnalysisContext();

      // Parse the statement
      parsedStmt = (StatementBase) parse();

      try {
        analyzer = ctx.createAnalyzer(makeTableCache());
        parsedStmt.analyze(analyzer);
      } catch (ImpalaException e) {
        fail(e.getMessage());
        // To keep the Java parser happy.
        throw new IllegalStateException(e);
      }
      return this;
    }

    private void makeAnalysisContext() {
      // Build a query context
      EventSequence timeline = new EventSequence("Frontend Test Timeline");
      ctx = new AnalysisContext(queryCtx,
          AuthorizationConfig.createAuthDisabledConfig(), timeline);
    }

    private StmtTableCache makeTableCache() {
      // Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
      StmtMetadataLoader mdLoader =
         new StmtMetadataLoader(frontend, ctx.getQueryCtx().session.database, null);
      try {
        return mdLoader.loadTables(parsedStmt);
      } catch (InternalException e) {
        fail(e.getMessage());
        // To keep the Java parser happy.
        throw new IllegalStateException(e);
      }
    }

    /**
     * Asserts that a warning is produced.
     */
    public void expectWarning(String expectedWarning) {
      List<String> actualWarnings = analysisResult.getAnalyzer().getWarnings();
      boolean matchedWarning = false;
      for (String actualWarning: actualWarnings) {
        if (actualWarning.startsWith(expectedWarning)) {
          matchedWarning = true;
          break;
        }
      }
      if (!matchedWarning) {
        fail(String.format("Did not produce expected warning.\n" +
            "Expected warning:\n%s.\nActual warnings:\n%s",
            expectedWarning, Joiner.on("\n").join(actualWarnings)));
      }
      Preconditions.checkNotNull(parsedStmt);
    }

    public TQueryCtx queryCtx() { return queryCtx; }
    public String stmt() { return stmt; }
    public ParseNode parseNode() { return parsedStmt; }
    public Analyzer analyzer() { return analyzer; }
    public SelectStmt selectStmt() {
      return (SelectStmt) parseNode();
    }
  }

  /**
   * Wraps an ExprRewriteRule to count how many times it's been applied.
   */
  private static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    private int rewrites;
    private final ExprRewriteRule wrapped;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped = wrapped;
    }

    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      Expr ret = wrapped.apply(expr, analyzer);
      if (expr != ret) { rewrites++; }
      return ret;
    }
  }

  /**
   * Test fixture to handle the common cases used during testing.
   * Provides access to various bits of the query analysis process
   * as needed to do verifications.
   */
  public class RewriteFixture {

    private TQueryOptions options;
    private String tableName = "functional.alltypessmall";
    private String exprStr;
    private boolean enableRewrites = true;
    private QueryFixture query;

    /**
     * Run the test using the standard options.
     */
    public RewriteFixture() { }

    /**
     * Run the test using specified options.
     */
    public RewriteFixture(TQueryOptions options) {
      this.options = options;
    }

    public RewriteFixture table(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public RewriteFixture disableRewrites() {
      enableRewrites = false;
      return this;
    }

    private void analyze(String stmtStr) throws AnalysisException {
      if (options == null) {
        query = AnalysisFixture.this.query(stmtStr);
      } else {
        query = AnalysisFixture.this.query(options, stmtStr);
      }
      if (enableRewrites) {
        query.analyze();
      } else {
        query.analyzeWithoutRewrite();
      }
    }

    /**
     * Analyze a query in which the given expression is placed in
     * the SELECT clause:
     * SELECT <expr> FROM ...
     */
    public RewriteFixture select(String expr) throws AnalysisException {
      this.exprStr = expr;
      String stmtStr = "select " + exprStr + " from " + tableName;
      analyze(stmtStr);
      return this;
    }

    /**
     * Return the parsed, analyzed expression resulting from a
     * {@link #select(String)} query.
     */
    public Expr selectExpr() {
      return query.selectStmt().getSelectList().getItems().get(0).getExpr();
    }

    /**
     * Verify that the {@link #select(String)} query produced the
     * expected result. Input is either null, meaning the expression
     * is unchanged, or a string that represents the toSql() form of
     * the rewritten SELECT expression.
     */
    public Expr verifySelect(String expectedExprStr) {
      Expr rewrittenExpr = selectExpr();
      String rewrittenSql = rewrittenExpr.toSql();
      assertEquals(expectedExprStr == null ? exprStr : expectedExprStr, rewrittenSql);
      return rewrittenExpr;
    }

    public Expr rewrite(Expr origExpr,
        List<ExprRewriteRule> rules,
        boolean requireFire) throws AnalysisException {

      // Wrap the rules in a (stateful) rule that counts the
      // number of times each wrapped rule fires.
      List<ExprRewriteRule> wrappedRules = Lists.newArrayList();
      for (ExprRewriteRule r : rules) {
        wrappedRules.add(new CountingRewriteRuleWrapper(r));
      }
      ExprRewriter rewriter = new ExprRewriter(wrappedRules);
      Expr rewrittenExpr = rewriter.rewrite(origExpr, query.analyzer());
      if (requireFire) {

        // Asserts that all specified rules fired at least once. This makes sure that
        // the rules being tested are, in fact, being executed. A common mistake is
        // to write an expression that's re-written by the constant folder before
        // getting to the rule that is intended for the test.
        for (ExprRewriteRule r : wrappedRules) {
          CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
          assertTrue("Rule " + w.wrapped.toString() + " didn't fire.",
            w.rewrites > 0);
        }
      }
      assertEquals(requireFire, rewriter.changed());
      return rewrittenExpr;
    }

    public Expr verifyExprEquivalence(Expr origExpr, String expectedExprStr,
        List<ExprRewriteRule> rules) throws AnalysisException {
      String origSql = origExpr.toSql();

      boolean expectChange = expectedExprStr != null;
      Expr rewrittenExpr = rewrite(origExpr, rules, expectChange);

      String rewrittenSql = rewrittenExpr.toSql();
      if (expectedExprStr != null) {
        assertEquals(expectedExprStr, rewrittenSql);
      } else {
        assertEquals(origSql, rewrittenSql);
      }
      return rewrittenExpr;
    }

    public Expr verifySelectRewrite(
        List<ExprRewriteRule> rules, String expectedExprStr)
        throws AnalysisException {
      return verifyExprEquivalence(selectExpr(), expectedExprStr, rules);
    }

    /**
     * Analyze a query in which the given expression is placed in
     * the WHERE clause:
     * SELECT count(1) FROM ... WHERE <expr>
     */
    public RewriteFixture where(String expr) throws AnalysisException {
      this.exprStr = expr;
      String stmtStr = "select count(1)  from " + tableName + " where " + exprStr;
      analyze(stmtStr);
      return this;
    }

    /**
     * Return the parsed, analyzed expression resulting from a
     * {@link #select(String)} query.
     */
    public Expr whereExpr() {
      return query.selectStmt().getWhereClause();
    }

    public Expr verifyWhereRewrite(
        List<ExprRewriteRule> rules, String expectedExprStr)
        throws AnalysisException {
      return verifyExprEquivalence(whereExpr(), expectedExprStr, rules);
    }

    /**
     * @return the query fixture used for this query.
     */
    public QueryFixture query() { return query; }
  }

  private final Frontend frontend;
  private String db = Catalog.DEFAULT_DB;
  private String user = System.getProperty("user.name");
  private TQueryOptions queryOptions;

  public AnalysisFixture(Frontend frontend) {
    this.frontend = frontend;
  }

  public void setDB(String db) {
    this.db = db;
  }

  public void setOptions(TQueryOptions queryOptions) {
    this.queryOptions = queryOptions;
  }

  public TQueryOptions getOptions() {
    if (queryOptions == null) {
      queryOptions = new TQueryOptions();
    }
    return queryOptions;
  }

  public TQueryOptions cloneOptions() {
    return new TQueryOptions(getOptions());
  }

  /**
   * Disable the optional expression rewrites.
   */
  public void disableExprRewrite() {
    getOptions().setEnable_expr_rewrites(false);
  }

  public TQueryCtx queryContext() {
    return TestUtils.createQueryContext(db, user, getOptions());
  }

  /**
   * Analyze a query and return a {@link QueryFixture} for it,
   * using the options defined in this fixture.
   */
  public QueryFixture query(String stmt) throws AnalysisException {
    return new QueryFixture(queryContext(), stmt);
  }

  /**
   * Analyze a query and return a {@link QueryFixture} for it,
   * using the specified options.
   */
  public QueryFixture query(TQueryOptions options, String stmt)
      throws AnalysisException {
    return new QueryFixture(
        TestUtils.createQueryContext(db, user, options), stmt);
  }

  public RewriteFixture rewriteFixture() {
    return new RewriteFixture();
  }

  public RewriteFixture rewriteFixture(TQueryOptions options) {
    return new RewriteFixture(options);
  }
}
