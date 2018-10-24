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
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.junit.Assert;

import com.google.common.collect.Lists;

/**
 * Base class for tests of rewrite rules. Provides functions that
 * parse and analyze a query, skipping rewrites. Then, invokes the
 * requested rewrite rules and verifies the result. The result is
 * checked by converting the revised expression back into text.
 * Note that literals will come back upper case. Verifies that the
 * target rules did, in fact, fire, unless the test wants to verify
 * that the rule did not fire by passing <code>null</code> as the
 * expected result.
 */
public abstract class BaseRewriteRulesTest extends FrontendTestBase {

  /** Wraps an ExprRewriteRule to count how many times it's been applied. */
  private static class CountingRewriteRuleWrapper implements ExprRewriteRule {
    int rewrites;
    ExprRewriteRule wrapped;

    CountingRewriteRuleWrapper(ExprRewriteRule wrapped) {
      this.wrapped = wrapped;
    }

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

  public Expr verifyPartialRewrite(String exprStr, List<ExprRewriteRule> rules, String expectedExprStr)
      throws ImpalaException {
    return verifyRewrites("functional.alltypessmall", exprStr, rules, expectedExprStr, false);
  }

  public Expr RewritesOk(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    return verifyRewrites(tableName, exprStr, rules, expectedExprStr, true);
  }

  private Expr verifyRewrites(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr, boolean requireFire) throws ImpalaException {
    String stmtStr = "select " + exprStr + " from " + tableName;
    // Analyze without rewrites since that's what we want to test here.
    SelectStmt stmt = (SelectStmt) ParsesOk(stmtStr);
    AnalyzesOkNoRewrite(stmt);
    Expr origExpr = stmt.getSelectList().getItems().get(0).getExpr();
    Expr rewrittenExpr =
        verifyExprEquivalence(origExpr, expectedExprStr, rules,
            stmt.getAnalyzer(), requireFire);
    return rewrittenExpr;
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
      List<ExprRewriteRule> rules, Analyzer analyzer
      ) throws AnalysisException {
    return verifyExprEquivalence(origExpr, expectedExprStr,
        rules, analyzer, true);
  }

  private Expr verifyExprEquivalence(Expr origExpr, String expectedExprStr,
      List<ExprRewriteRule> rules, Analyzer analyzer,
      boolean requireFire) throws AnalysisException {
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
      if (requireFire) {
        // Asserts that all specified rules fired at least once. This makes sure that the
        // rules being tested are, in fact, being executed. A common mistake is to write
        // an expression that's re-written by the constant folder before getting to the
        // rule that is intended for the test.
        for (ExprRewriteRule r : wrappedRules) {
          CountingRewriteRuleWrapper w = (CountingRewriteRuleWrapper) r;
          Assert.assertTrue("Rule " + w.wrapped.toString() + " didn't fire.",
            w.rewrites > 0);
        }
      }
    } else {
      assertEquals(origSql, rewrittenSql);
    }
    Assert.assertEquals(expectChange, rewriter.changed());
    return rewrittenExpr;
  }

  /**
   * Helper for prettier error messages than what JUnit.Assert provides.
   */
  private void assertEquals(String expected, String actual) {
    if (!actual.equals(expected)) {
      Assert.fail(String.format("\nActual: %s\nExpected: %s\n", actual, expected));
    }
  }

}
