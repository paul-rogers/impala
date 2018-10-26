package org.apache.impala.analysis;

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.RewriteConditionalFnsRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Test RewriteConditionalFnsRule. Tests  the basic
 * rewrite. See {@link FullRewriteTest} for compound
 * simplifications.
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

  @Test
  public void testNvl2() throws ImpalaException {
    RewritesOk("nvl2(id, 10, 20)", rule,
        "CASE WHEN id IS NOT NULL THEN 10 ELSE 20 END");

    // Optimizations handled in CASE, not in basic rewrite
    RewritesOk("nvl2(null, 10, 20)", rule,
        "CASE WHEN NULL IS NOT NULL THEN 10 ELSE 20 END");
  }

  /**
   * Test nullif(expr1, expr2).
   *
   * Returns NULL if the two specified arguments are equal. If the specified
   * arguments are not equal, returns the value of expr1.
   */
  @Test
  public void testNullIf() throws ImpalaException {
    // Nothing is equal to null, so return expr1, which is NULL.
    RewritesOk("nullif(null, id)", rule, "NULL");
    // Nothing is equal to null, so return expr1, which is id.
    RewritesOk("nullif(id, null)", rule, "id");
    // Otherwise, rewrite to a CASE statement.
    RewritesOk("nullif(id, 10)", rule,
        "CASE WHEN id IS DISTINCT FROM 10 THEN id END");
  }

  /**
   * Test that complex BETWEEN conditions that use if() are
   * further rewritten to use CASE.
   */
  @Test
  public void testNestedBetween() throws ImpalaException {

    // See ExprRewriteRuleTest.TestBetweenToCompoundRule
    // For test of rewrite of BETWEEN to use if
    // Here we verify the full rewrite to CASE

    List<ExprRewriteRule> rules = Lists.newArrayList(
        BetweenToCompoundRule.INSTANCE,
        rule);
    RewritesOk(
        "int_col not between if(tinyint_col not between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)", rules,
        "int_col < CASE WHEN tinyint_col < 1 OR tinyint_col > 2 THEN 10 ELSE 20 END " +
        "OR int_col > CAST(smallint_col < 1 OR smallint_col > 2 AS INT)");
    // Mixed nested BETWEEN and NOT BETWEEN predicates.
    RewritesOk(
        "int_col between if(tinyint_col between 1 and 2, 10, 20) " +
        "and cast(smallint_col not between 1 and 2 as int)", rules,
        "int_col >= CASE WHEN tinyint_col >= 1 AND tinyint_col <= 2 THEN 10 ELSE 20 END " +
        "AND int_col <= CAST(smallint_col < 1 OR smallint_col > 2 AS INT)");
  }
}
