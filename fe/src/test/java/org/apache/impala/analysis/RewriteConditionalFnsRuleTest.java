package org.apache.impala.analysis;

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.RewriteConditionalFnsRule;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Test RewriteConditionalFnsRule. Tests both the basic
 * rewrite, and the extended rewrite + CASE simplification.
 */
public class RewriteConditionalFnsRuleTest extends BaseRewriteRulesTest {

  ExprRewriteRule rule = RewriteConditionalFnsRule.INSTANCE;

  @Test
  public void testIf() throws ImpalaException {

    // Basic rewrite, no simplification
    RewritesOk("if(true, id, id+1)", rule,
        "CASE WHEN TRUE THEN id ELSE id + 1 END");

    // Simplifications provided by CASE rewriting
    List<ExprRewriteRule> rules = Lists.newArrayList(
        rule,
        SimplifyConditionalsRule.INSTANCE);
    RewritesOk("if(true, id, id+1)", rules, "id");
    RewritesOk("if(false, id, id+1)", rules, "id + 1");
    RewritesOk("if(null, id, id+1)", rules, "id + 1");
    // Nothing to simplify
    verifyPartialRewrite("if(id = 0, true, false)", rules,
        "CASE WHEN id = 0 THEN TRUE ELSE FALSE END");
    // Don't simplify if drops last aggregate
    verifyPartialRewrite("if(true, 0, sum(id))", rules,
        "CASE WHEN TRUE THEN 0 ELSE sum(id) END");
    verifyPartialRewrite("if(false, sum(id), 0)", rules,
        "CASE WHEN FALSE THEN sum(id) ELSE 0 END");
    // OK to simplify if retains an aggregate
    RewritesOk("if(false, max(id), min(id))", rules, "min(id)");
    RewritesOk("if(true, max(id), min(id))", rules, "max(id)");
    verifyPartialRewrite("if(false, 0, sum(id))", rules, "sum(id)");
    verifyPartialRewrite("if(true, sum(id), 0)", rules, "sum(id)");
    // If else is NULL, case should drop it since it is
    // the default
    // IMPALA-7750
//    RewritesOk("if(id = 0, 10, null)", rules,
//        "CASE WHEN id = 0 THEN 10 END");
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

      // Can't simplify if removes only aggregate
      RewritesOk(f + "(null, max(id))", rule, "max(id)");
      RewritesOk(f + "(1, max(id))", rule,
          "CASE WHEN 1 IS NULL THEN max(id) ELSE 1 END");
    }
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

  @Test
  public void testNvl2() throws ImpalaException {
    RewritesOk("nvl2(null, 10, 20)", rule, "20");
    RewritesOk("nvl2(null, 10, sum(id))", rule, "sum(id)");
    RewritesOk("nvl2(null, avg(id), sum(id))", rule, "sum(id)");
    RewritesOk("nvl2(4, 10, 20)", rule, "10");
    RewritesOk("nvl2(4, sum(id), 20)", rule, "sum(id)");
    RewritesOk("nvl2(4, sum(id), avg(id))", rule, "sum(id)");
    RewritesOk("nvl2(id, 10, 20)", rule,
        "CASE WHEN id IS NOT NULL THEN 10 ELSE 20 END");
    RewritesOk("nvl2(id, 10, 20)", rule,
        "CASE WHEN id IS NOT NULL THEN 10 ELSE 20 END");
    RewritesOk("nvl2(null, sum(id), 20)", rule,
        "CASE WHEN NULL IS NOT NULL THEN 10 ELSE sum(id) END");
    RewritesOk("nvl2(4, avg(id), 20)", rule,
        "CASE WHEN 4 IS NOT NULL THEN avg(id) ELSE 20 END");
  }

  @Test
  public void testNullIf() throws ImpalaException {
    RewritesOk("nullif(null, id)", rule, "NULL");
    RewritesOk("nullif(id, null)", rule, "NULL");
    RewritesOk("nullif(id, 10)", rule,
        "CASE WHEN id IS DISTINCT FROM 10 THEN id END");
  }
}
