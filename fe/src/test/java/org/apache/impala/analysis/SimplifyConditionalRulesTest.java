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
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests SimplifyConditionalsRule.
 */
public class SimplifyConditionalRulesTest extends BaseRewriteRulesTest {

  ExprRewriteRule rule = SimplifyConditionalsRule.INSTANCE;
  List<ExprRewriteRule> rules = Lists.newArrayList();

  public SimplifyConditionalRulesTest() {
    rules.add(FoldConstantsRule.INSTANCE);
    rules.add(rule);
  }

  @Test
  public void testCompoundPredicate() throws ImpalaException {

    // CompoundPredicate
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
  public void testCaseWithoutExpr() throws ImpalaException {

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

    // If the following works, can remove an optimization for IfNull
    // See IMPALA-7750
//    RewritesOk("CASE WHEN NULL IS NULL THEN 10 ELSE 20 END", rules, "10");
//    RewritesOk("CASE WHEN 10 IS NULL THEN 10 ELSE 20 END", rules, "20");
  }

  @Test
  public void testSimplifyDecode() throws ImpalaException {

    // DECODE
    // Single TRUE case with no preceding non-constant case.
    RewritesOk("decode(1, 0, id, 1, id + 1, 2, id + 2)", rules, "id + 1");

    // Single TRUE case with predecing non-constant case.
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

    // When NULL - DECODE allows the decodeExpr to equal NULL (see CaseExpr.java),
    // so the NULL case is not treated as a constant FALSE and removed.
    RewritesOk("decode(id, null, 0, 1)", rules, null);

    // All non-constant, don't rewrite.
    RewritesOk("decode(id, 1, 1, 2, 2)", rules, null);
  }

  @Test
  public void testImpala5125() throws ImpalaException {

    // IMPALA-5125: Exprs containing aggregates should not be rewritten if the rewrite
    // eliminates all aggregates.
    RewritesOk("true || sum(id) = 0", rule, null);
    RewritesOk("case when true then 0 when false then sum(id) end", rule, null);
    RewritesOk(
        "case when true then count(id) when false then sum(id) end",
        rule, "count(id)");
  }

}
