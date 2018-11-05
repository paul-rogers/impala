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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.collect.Lists;

/**
 * Test fixture to handle the common cases used during testing.
 * Provides access to various bits of the query analysis process
 * as needed to do verifications.
 *
 * Builds on the QueryFixture to add additional
 * tools needed to probe rewrites of the various clauses within a
 * query.
 */
public class RewriteFixture {

  /**
   * Wraps an ExprRewriteRule to count how many times it's been applied.
   */
  static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    int rewrites_;
    final ExprRewriteRule wrapped_;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped_ = wrapped;
    }

    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
      Expr ret = wrapped_.apply(expr, analyzer);
      if (expr != ret) { rewrites_++; }
      return ret;
    }
  }

  private final AnalysisFixture analysisFixture_;
  private TQueryOptions options_;
  private String tableName_ = "functional.alltypessmall";
  private String exprStr_;
  private boolean enableRewrites_ = true;
  private QueryFixture query_;

  /**
   * Run the test using the standard options.
   * @param analysisFixture TODO
   */
  public RewriteFixture(AnalysisFixture analysisFixture) {
    this.analysisFixture_ = analysisFixture; }

  /**
   * Run the test using specified options.
   */
  public RewriteFixture(AnalysisFixture analysisFixture, TQueryOptions options) {
    this.analysisFixture_ = analysisFixture;
    this.options_ = options;
  }

  public RewriteFixture table(String tableName) {
    this.tableName_ = tableName;
    return this;
  }

  public RewriteFixture disableRewrites() {
    enableRewrites_ = false;
    return this;
  }

  private void analyze(String stmtStr) throws AnalysisException {
    if (options_ == null) {
      query_ = this.analysisFixture_.query(stmtStr);
    } else {
      query_ = this.analysisFixture_.query(options_, stmtStr);
    }
    if (enableRewrites_) {
      query_.analyze();
    } else {
      query_.analyzeWithoutRewrite();
    }
  }

  /**
   * Analyze a query in which the given expression is placed in
   * the SELECT clause:
   * SELECT <expr> FROM ...
   */
  public RewriteFixture select(String expr) throws AnalysisException {
    this.exprStr_ = expr;
    String stmtStr = "select " + exprStr_ + " from " + tableName_;
    analyze(stmtStr);
    return this;
  }

  /**
   * Return the parsed, analyzed expression resulting from a
   * {@link #select(String)} query.
   */
  public Expr selectExpr() {
    return query_.selectStmt().getSelectList().getItems().get(0).getExpr();
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
    assertEquals(expectedExprStr == null ? exprStr_ : expectedExprStr, rewrittenSql);
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
    Expr rewrittenExpr = rewriter.rewrite(origExpr, query_.analyzer());
    if (requireFire) {

      // Asserts that all specified rules fired at least once. This makes sure that
      // the rules being tested are, in fact, being executed. A common mistake is
      // to write an expression that's re-written by the constant folder before
      // getting to the rule that is intended for the test.
      for (ExprRewriteRule r : wrappedRules) {
        CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
        assertTrue("Rule " + w.wrapped_.toString() + " didn't fire.",
          w.rewrites_ > 0);
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
    this.exprStr_ = expr;
    String stmtStr = "select count(1)  from " + tableName_ + " where " + exprStr_;
    analyze(stmtStr);
    return this;
  }

  /**
   * Return the parsed, analyzed expression resulting from a
   * {@link #select(String)} query.
   */
  public Expr whereExpr() {
    return query_.selectStmt().getWhereClause();
  }

  public Expr verifyWhereRewrite(
      List<ExprRewriteRule> rules, String expectedExprStr)
      throws AnalysisException {
    return verifyExprEquivalence(whereExpr(), expectedExprStr, rules);
  }

  /**
   * @return the query fixture used for this query.
   */
  public QueryFixture query() { return query_; }
}