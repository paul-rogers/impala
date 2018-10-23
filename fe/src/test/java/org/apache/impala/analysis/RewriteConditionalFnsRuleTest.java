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

import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.RewriteConditionalFnsRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests RewriteConditionalFnsRule.
 */
public class RewriteConditionalFnsRuleTest extends BaseRewriteRulesTest {

  ExprRewriteRule rule = RewriteConditionalFnsRule.INSTANCE;

  @Test
  public void testIf() throws ImpalaException {

    // Basic rewrite, no simplification
    RewritesOk("if(true, id, id+1)", rule,
        "CASE WHEN TRUE THEN id ELSE id + 1 END");
  }

  /**
   * IfNull and its aliases are rewritten to use case expressions.
   */
  @Test
  public void testifNull() throws ImpalaException {
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      RewritesOk(f + "(id, id + 1)", rule,
          "CASE WHEN id IS NULL THEN id + 1 ELSE id END");
    }
  }

  public static String coalesceRewrite(String...terms) {
    StringBuilder buf = new StringBuilder()
        .append("CASE");
    for (int i = 0; i < terms.length -1; i++) {
      String term = terms[i];
      buf
        .append(" WHEN ")
        .append(term)
        .append(" IS NOT NULL THEN ")
        .append(term);
    }
    return buf
        .append(" ELSE ")
        .append(terms[terms.length - 1])
        .append(" END")
        .toString();
  }

  /**
   * Test COALSESCE(a, b, c, ...). The basic rewrite handles
   * many optimizations because these are too complex to be
   * done once rewritten to a CASE expression.
   */
  @Test
  public void testCoalesce() throws ImpalaException {

    // Base case
    RewritesOk("coalesce(id)", rule, "id");
    RewritesOk("COALESCE(id, year)", rule,
        coalesceRewrite("id", "year"));
    RewritesOk("coalesce(id, year, 10)", rule,
        coalesceRewrite("id", "year", "10"));

    // IMPALA-5016: Simplify COALESCE function
    // Test skipping nulls.
    RewritesOk("coalesce(null, id, year)", rule, coalesceRewrite("id", "year"));
    RewritesOk("coalesce(null, id, null, year, null)", rule,
        coalesceRewrite("id", "year"));
    RewritesOk("coalesce(null, 1, id)", rule, "1");
    RewritesOk("coalesce(null, null, id, null)", rule, "id");
    RewritesOk("coalesce(null, null)", rule, "NULL");

    // If the leading parameter is a non-NULL constant, rewrite to that constant.
    RewritesOk("coalesce(1, id, year)", rule, "1");

    // If non-leading parameter is a non-NULL constant, drop other terms.
    RewritesOk("coalesce(id, 1, year)", rule, coalesceRewrite("id", "1"));

    // If COALESCE has only one parameter, rewrite to the parameter.
    RewritesOk("coalesce(id)", rule, "id");

    // If all parameters are NULL, rewrite to NULL.
    RewritesOk("coalesce(null, null)", rule, "NULL");

    // Do not rewrite non-literal constant exprs, rely on constant folding.
    RewritesOk("coalesce(null is null, id)", rule, coalesceRewrite("NULL IS NULL", "id"));
    RewritesOk("coalesce(10 + null, id)", rule, coalesceRewrite("10 + NULL", "id"));

    // Combine COALESCE rule with FoldConstantsRule.
    List<ExprRewriteRule> rules = Lists.newArrayList(
        FoldConstantsRule.INSTANCE,
        rule);
    RewritesOk("coalesce(1 + 2, id, year)", rules, "3");
    RewritesOk("coalesce(null is null, bool_col)", rules, "TRUE");
    RewritesOk("coalesce(10 + null, id, year)", rules, coalesceRewrite("id", "year"));

    // Don't rewrite based on nullability of slots. TODO (IMPALA-5753).
    RewritesOk("coalesce(year, id)", rule, coalesceRewrite("year", "id"));
    RewritesOk("functional_kudu.alltypessmall", "coalesce(id, year)",
        rule, coalesceRewrite("id", "year"));

    // IMPALA-7419: coalesce that gets simplified and contains an aggregate
    RewritesOk("coalesce(null, min(distinct tinyint_col), 42)", rule,
        coalesceRewrite("min(tinyint_col)", "42"));
    RewritesOk("coalesce(null, 42, min(distinct tinyint_col))", rule,
        coalesceRewrite("42", "min(tinyint_col)"));
    RewritesOk("coalesce(42, null, min(distinct tinyint_col))", rule,
        coalesceRewrite("42", "min(tinyint_col)"));
  }

  /**
   * nvl2 is rewritten to if in the parser, then to case
   * in the rewrite engine
   */
  @Test
  public void testNvl2() throws ImpalaException {
    RewritesOk("nvl2(id, 10, 20)", rule,
        "CASE WHEN id IS NOT NULL THEN 10 ELSE 20 END");

    // Optimizations handled in CASE, not in basic rewrite
    RewritesOk("nvl2(null, 10, 20)", rule,
        "CASE WHEN NULL IS NOT NULL THEN 10 ELSE 20 END");
  }

  /**
   * nullif is rewritten to if in the parser, then to case
   * in the rewrite engine
   */
  @Test
  public void testNullIf() throws ImpalaException {
    RewritesOk("nullif(id, 10)", rule,
        "CASE WHEN id IS DISTINCT FROM 10 THEN id ELSE NULL END");
    // TODO: Omit NULL ELSE clause
//    RewritesOk("nullif(id, 10)", rule,
//        "CASE WHEN id IS DISTINCT FROM 10 THEN id END");
  }
}
