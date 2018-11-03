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

import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SimplifyConditionalsRuleTest extends BaseRewriteRulesTest {
  ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;

  @Test
  public void testIf() throws ImpalaException {

    RewritesOk("if(true, id, id+1)", rule, "id");
    RewritesOk("if(false, id, id+1)", rule, "id + 1");
    RewritesOk("if(null, id, id+1)", rule, "id + 1");
    RewritesOk("if(id = 0, true, false)", rule, null);

    // Test if(CAST(NULL AS boolean), ...)
    RewritesOk("if(cast(null as boolean), id, id+1)", rule, "id + 1");
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
    RewritesOk("if(null and true, id, id+1)", rules, "id + 1");
  }

  // IFNULL and its aliases
  @Test
  public void testIfNull() throws ImpalaException {

    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      RewritesOk(f + "(null, id)", rule, "id");
      RewritesOk(f + "(null, null)", rule, "NULL");
      RewritesOk(f + "(id, id + 1)", rule, null);

      RewritesOk(f + "(1, 2)", rule, "1");
      RewritesOk(f + "(0, id)", rule, "0");
      // non literal constants shouldn't be simplified by the rule
      RewritesOk(f + "(1 + 1, id)", rule, null);
      RewritesOk(f + "(NULL + 1, id)", rule, null);

      // But should be simplified with constant folding
      RewritesOk(f + "(1 + 1, id)", rules, "2");
      RewritesOk(f + "(NULL + 1, id)", rules, "id");
      RewritesOk(f + "(cast(null as int), id)", rule, "id");
    }
  }

  @Test
  public void testCompoundPredicates() throws ImpalaException {

    RewritesOk("false || id = 0", rule, "id = 0");
    RewritesOk("true || id = 0", rule, "TRUE");
    RewritesOk("false && id = 0", rule, "FALSE");
    RewritesOk("true && id = 0", rule, "id = 0");
    // NULL with a non-constant other child doesn't get rewritten.
    RewritesOk("null && id = 0", rule, null);
    RewritesOk("null || id = 0", rule, null);
  }

  @Test
  public void testCaseWithExpr() throws ImpalaException {

    // Single TRUE case with no preceding non-constant cases.
    RewritesOk("case 1 when 0 then id when 1 then id + 1 when 2 then id + 2 end",
        rule, "id + 1");

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
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    RewritesOk("case 1 when id then id when 2 - 1 then id + 1 when 1 then id + 2 end",
        rules, "CASE 1 WHEN id THEN id ELSE id + 1 END");

    // When NULL.
    RewritesOk("case 0 when null then id else 1 end", rule, "1");

    // All non-constant, don't rewrite.
    RewritesOk("case id when 1 then 1 when 2 then 2 else 3 end", rule, null);

    // Null as expression
    // Nothing is equal to null, even null
    RewritesOk("case null when null then 10 else 20 end", rule, "20");
    RewritesOk("case null when 10 then 10 else 20 end", rule, "20");
    Expr result = RewritesOk("case null when 10 then 10 end", rule, "NULL");

    // Null must have the same type as the 10:
    Assert.assertEquals(Type.TINYINT, result.getType());

    // Trim trivial else
    RewritesOk("case id when 1 then 2 else null end", rule,
        "CASE id WHEN 1 THEN 2 END");
    result = RewritesOk("case 1 when null then 5 when 1 then null end", rule,
        "NULL");

    // Null must have the same type as the 5:
    Assert.assertEquals(Type.TINYINT, result.getType());
  }

  @Test
  public void testCaseWithoutExpr() throws ImpalaException {

    // Single TRUE case with no preceding non-constant case.
    RewritesOk("case when FALSE then 0 when TRUE then 1 end", rule, "1");

    // Single TRUE case with preceding non-constant case.
    RewritesOk("case when id = 0 then 0 when true then 1 when id = 2 then 2 end",
        rule,
        "CASE WHEN id = 0 THEN 0 ELSE 1 END");

    // Single FALSE case.
    RewritesOk("case when id = 0 then 0 when false then 1 when id = 2 then 2 end",
        rule,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");

    // Multiple FALSE case
    RewritesOk("case when false then 3 when id = 0 then 0 when false then 1 " +
        "when id = 2 then 2 when false then 4 end",
        rule,
        "CASE WHEN id = 0 THEN 0 WHEN id = 2 THEN 2 END");

    // All FALSE, return ELSE.
    RewritesOk(
        "case when false then 1 when false then 2 else id + 1 end", rule, "id + 1");

    // All FALSE, return implicit NULL ELSE.
    Expr result = RewritesOk("case when false then 0 end", rule, "NULL");

    // Null must have the same type as the 0:
    Assert.assertEquals(Type.TINYINT, result.getType());

    // Multiple TRUE, first one becomes ELSE.
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    RewritesOk("case when id = 1 then 0 when 2 = 1 + 1 then 1 when true then 2 end",
        rules, "CASE WHEN id = 1 THEN 0 ELSE 1 END");

    // When NULL.
    RewritesOk("case when id = 0 then 0 when null then 1 else 2 end", rule,
        "CASE WHEN id = 0 THEN 0 ELSE 2 END");

    // When CAST(NULL AS type)
    RewritesOk("case when cast(null AS boolean) then 0 else 2 end", rule,
        "2");
    // All constant: done in null-folding rule
    RewritesOk("case when null and true then 0 else 2 end",
        FoldConstantsRule.INSTANCE,
        "2");
    // Null cast to boolean, non-literal terms
    RewritesOk("case when null and true then id else 2 end", rules,
        "2");

    // All non-constant, don't rewrite.
    RewritesOk("case when id = 0 then 0 when id = 1 then 1 end", rule, null);

    // Trim trivial else
    RewritesOk("case when id = 1 then 2 else null end", rule,
        "CASE WHEN id = 1 THEN 2 END");

    // Trim trivial else, trim trailing THEN NULL
    result = RewritesOk("case when null then 5 when id = 1 then null end", rule,
        "NULL");

    // Null must have the same type as the 5:
    Assert.assertEquals(Type.TINYINT, result.getType());

    RewritesOk("case when id = 1 then 2 when id = 2 then null " +
        "when null then 20 else null end",
        rule,
        "CASE WHEN id = 1 THEN 2 END");
  }

  @Test
  public void testDecode() throws ImpalaException {

    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);

    // Single TRUE case with no preceding non-constant case.
    RewritesOk("decode(1, 0, id, 1, id + 1, 2, id + 2)", rules, "id + 1");

    // Single TRUE case with preceding non-constant case.
    RewritesOk("decode(1, id, id, 1, id + 1, 0)", rules,
        "CASE WHEN 1 = id THEN id ELSE id + 1 END");

    // Single FALSE case.
    RewritesOk("decode(1, 0, id, tinyint_col, id + 1)", rules,
        "CASE WHEN 1 = tinyint_col THEN id + 1 END");

    // All FALSE, return ELSE.
    RewritesOk("decode(1, 0, id, 2, 2, 3)", rules, "3");

    // All FALSE, return implicit NULL ELSE.
    RewritesOk("decode(1, 1 + 1, id, 1 + 2, 3)", rules, "NULL");

    // Multiple TRUE, first one becomes ELSE.
    RewritesOk("decode(1, id, id, 1 + 1, 0, 1 * 1, 1, 2 - 1, 2)", rules,
        "CASE WHEN 1 = id THEN id ELSE 1 END");

    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java), so the
    // NULL case is not treated as a constant FALSE and removed.
    RewritesOk("decode(id, null, 0, 1)", rules, null);

    // All non-constant, don't rewrite.
    RewritesOk("decode(id, 1, 1, 2, 2)", rules, null);
  }

  @Test
  public void testImpala5125() throws ImpalaException {

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    RewritesOk("if(true, 0, sum(id))", rule, null);
    RewritesOk("if(false, max(id), min(id))", rule, "min(id)");
    RewritesOk("true || sum(id) = 0", rule, null);
    RewritesOk("ifnull(null, max(id))", rule, "max(id)");
    RewritesOk("ifnull(1, max(id))", rule, null);
    RewritesOk("case when true then 0 when false then sum(id) end", rule, null);
    RewritesOk(
        "case when true then count(id) when false then sum(id) end", rule, "count(id)");
  }

  @Test
  public void testCoalesce() throws ImpalaException {

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
    List<ExprRewriteRule> rules = Lists.newArrayList();
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
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
}
