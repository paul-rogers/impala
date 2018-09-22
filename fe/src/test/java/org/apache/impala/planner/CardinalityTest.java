package org.apache.impala.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the inference of tuple cardinality from NDV and
 * selectivity.
 */
public class CardinalityTest extends PlannerTestBase {

  private static final boolean DEBUG_MODE = true;

  /**
   * Test the happy path: table with stats, no all-null cols.
   */
  @Test
  public void testBasicsWithStats() {

    // Return all rows. Cardinality is row count;
    runTest("SELECT id FROM functional.alltypes;", 7300);

    // Return all rows. Cardinality is row count,
    // should not be influenced by limited NDV of selected
    // column.
    runTest("SELECT bool_col FROM functional.alltypes;", 7300);

    // Result cardinality reduced by limited NDV.
    // Boolean column has cardinality 3 (true, false, null).
    // Since we have metadata, and know the column is non-null,
    // NDV is 2. We select one of them.
    runTest("SELECT id FROM functional.alltypes WHERE bool_col = TRUE;", 7300/2);

    // Result cardinality reduced by NDV.
    // NDV should be 10 (from metadata) + 1 (for nulls).
    // 664 =~ 7300/11
    runTest("SELECT id FROM functional.alltypes WHERE int_col = 1;", 664);

    // Assume classic 0.1 selectivity for other operators
    // IMPALA-7560 says this should be revised.
    runTest("SELECT id FROM functional.alltypes WHERE int_col != 1", 730);

    // IMPALA-7601 says the following should be revised.
    runTest("SELECT id FROM functional.alltypes WHERE int_col > 1", 730);
    runTest("SELECT id FROM functional.alltypes WHERE int_col > 1", 730);

    // Not using NDV of int_col, instead uses default 0.1 value.
    // Since NULL is one of the NDV's, nullvalue() is true
    // 1/NDV of the time, on average.
    runTest("SELECT id FROM functional.alltypes WHERE nullvalue(int_col)", 730);

    // Grouping should reduce cardinality
    runTest("SELECT COUNT(*) FROM functional.alltypes GROUP BY int_col", 11);
    runTest("SELECT COUNT(*) FROM functional.alltypes GROUP BY id", 7300);
    runTest("SELECT COUNT(*) FROM functional.alltypes GROUP BY bool_col", 2);
  }

  /**
   * Test tables with all-null columns. After IMPALA-7310,
   * NDV of an all-null column should be 1. (Before NDV was undefined,
   * so cardinality was undefined.)
   */
  @Test
  public void testNulls() {
    runTest("SELECT d FROM functional.nullrows", 26);
    // a has unique values, so NDV = 26
    runTest("SELECT d FROM functional.nullrows WHERE a = 'x'", 1);
    // f repeats for 5 rows, so NDV=6, 26/6 =~ 4
    runTest("SELECT d FROM functional.nullrows WHERE f = 'x'", 4);
    // Revised use of nulls per IMPALA-7310
    // c is all nulls, NDV = 1, selectivity = 1/1, cardinality = 26
    runTest("SELECT d FROM functional.nullrows WHERE c = 'x'", 26);
    // NDV multiplied out on group by
    runTest("SELECT d FROM functional.alltypes, functional.nullrows", 7300 * 26);
    String baseStmt = "SELECT COUNT(*) " +
                      "FROM functional.alltypes, functional.nullrows " +
                      "GROUP BY ";
    runTest(baseStmt + "id", 7300);
    runTest(baseStmt + "a", 26);
    runTest(baseStmt + "f", 6);
    runTest(baseStmt + "c", 1);
    runTest(baseStmt + "a, c", 26);
  }

  /**
   * Joins should multiply out cardinalities.
   */
  @Test
  public void testJoins() {
    // Cartesian product
    String joinClause = " FROM functional.alltypes t1, functional.alltypes t2 ";
    runTest("SELECT t1.id" + joinClause, 7300 * 7300);
    // Cartesian product, reduced by NDV of group key
    runTest("SELECT COUNT(*)" + joinClause + "GROUP BY t1.id", 7300);
    runTest("SELECT COUNT(*)" + joinClause + "GROUP BY t1.id, t1.int_col", 7300 * 11);
  }

  @Test
  public void testBasicsWithoutStats() {
    // Before IMPALA-7608, no cardinality is available (result is -1)
    // After IMPALA-7608, cardinality is estimated from file size.
    runTest("SELECT a FROM functional.tinytable;", 1);
  }

  protected void runTest(String query, long expected) {
    List<PlanFragment> plan = getPlan(query);
    PlanNode planRoot = plan.get(0).getPlanRoot();
    assertEquals(expected, planRoot.getCardinality());
  }

  private List<PlanFragment> getPlan(String query) {
    // Set up the query context. Note that we need to deep copy it before planning each
    // time since planning modifies it.
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    TQueryOptions queryOptions = queryCtx.client_request.getQuery_options();
    queryOptions.setNum_nodes(1);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.capturePlan();

    // Discard the actual execution plan. Return the cached
    // internal form instead.

    try {
      frontend_.createExecRequest(planCtx);
    } catch (ImpalaException e) {
      fail(e.getMessage());
    }
    List<PlanFragment> plan = planCtx.getPlan();
    if (DEBUG_MODE) {
      System.out.println(plan.get(0).getExplainString(queryOptions, TExplainLevel.EXTENDED));
    }
    return plan;
  }

}
