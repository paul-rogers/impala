package org.apache.impala.analysis;

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
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
  public void testIf() throws ImpalaException {

    // IF
    RewritesOk("if(true, id, id+1)", rule, "id");
    RewritesOk("if(false, id, id+1)", rule, "id + 1");
    RewritesOk("if(null, id, id+1)", rule, "id + 1");
    RewritesOk("if(id = 0, true, false)", rule, null);
  }

  /**
   * IfNull and its aliases are rewritten to use case expressions.
   */
  @Test
  public void testifNull() throws ImpalaException {
    // IFNULL and its aliases
    for (String f : ImmutableList.of("ifnull", "isnull", "nvl")) {
      RewritesOk(f + "(null, id)", rule, "id");
      RewritesOk(f + "(null, null)", rule, "NULL");
      RewritesOk(f + "(id, id + 1)", rule,
          "CASE WHEN id IS NULL THEN id + 1 ELSE id END");

      RewritesOk(f + "(1, 2)", rule, "1");
      RewritesOk(f + "(0, id)", rule, "0");

      // non literal constants shouldn't be simplified by the rule
      // But isnull, nvl should be replaced by ifnull,
      // then ifnull replaced by CASE
      RewritesOk(f + "(1 + 1, id)", rule,
          "CASE WHEN 1 + 1 IS NULL THEN id ELSE 1 + 1 END");
      RewritesOk(f + "(NULL + 1, id)", rule,
          "CASE WHEN NULL + 1 IS NULL THEN id ELSE NULL + 1 END");
    }
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

  private static String coalesceRewrite(String...terms) {
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
    // If non-leading parameter is a non-NULL constant, drop other terms
    RewritesOk("coalesce(id, 1, year)", rule, coalesceRewrite("id", "1"));

    // If COALESCE has only one parameter, rewrite to the parameter.
    RewritesOk("coalesce(id)", rule, "id");

    // If all parameters are NULL, rewrite to NULL.
    RewritesOk("coalesce(null, null)", rule, "NULL");

    // Do not rewrite non-literal constant exprs, rely on constant folding.
    RewritesOk("coalesce(null is null, id)", rule, coalesceRewrite("NULL IS NULL", "id"));
    RewritesOk("coalesce(10 + null, id)", rule, coalesceRewrite("10 + NULL", "id"));

    // Combine COALESCE rule with FoldConstantsRule.
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
  }

  @Test
  public void testNvl2() throws ImpalaException {
    RewritesOk("nvl2(null, 10, 20)", rule, "20");
    RewritesOk("nvl2(4, 10, 20)", rule, "10");
    RewritesOk("nvl2(id, 10, 20)", rule,
        "CASE WHEN id IS NOT NULL THEN 10 ELSE 20 END");
  }

  @Test
  public void testNullIf() throws ImpalaException {
    RewritesOk("nullif(null, id)", rule, "NULL");
    RewritesOk("nullif(id, null)", rule, "NULL");
    RewritesOk("nullif(id, 10)", rule,
        "CASE WHEN id IS DISTINCT FROM 10 THEN id END");
  }
}
