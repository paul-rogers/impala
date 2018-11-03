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

import org.apache.impala.analysis.AnalysisFixture.RewriteFixture;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;

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

  public AnalysisFixture fixture = new AnalysisFixture(frontend_);

  public Expr RewritesOk(String exprStr, ExprRewriteRule rule, String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rule, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, ExprRewriteRule rule,
      String expectedExprStr)
      throws ImpalaException {
    return RewritesOk(tableName, exprStr, Lists.newArrayList(rule), expectedExprStr);
  }

  public Expr RewritesOk(String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr)
      throws ImpalaException {
    return RewritesOk("functional.alltypessmall", exprStr, rules, expectedExprStr);
  }

  public Expr RewritesOk(String tableName, String exprStr, List<ExprRewriteRule> rules,
      String expectedExprStr) throws ImpalaException {
    RewriteFixture qf = fixture.rewriteFixture()
        .disableRewrites()
        .table(tableName)
        .select(exprStr);
    return qf.verifySelectRewrite(rules, expectedExprStr);
  }
}
