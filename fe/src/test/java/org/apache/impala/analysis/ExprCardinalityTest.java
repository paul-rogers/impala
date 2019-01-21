package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisSessionFixture;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.CardinalityTest;
import org.junit.Test;

/**
 * Tests expression cardinality and selectivity, both of which are
 * important inputs to scan and join cardinality estimates. See
 * also {@link ExprNdvTest}, {@link CardinalityTest}.
 */
public class ExprCardinalityTest {
  public static AnalysisSessionFixture session_ = new AnalysisSessionFixture();

  private void checkColumn(Table table, String colName,
      long expectedNdv, long expectedNullCount) {
    Column col = table.getColumn(colName);
    assertNotNull(col);
    ColumnStats stats = col.getStats();
    assertNotNull(stats);
    assertEquals(expectedNdv, stats.getNumDistinctValues());
    assertEquals(expectedNullCount, stats.getNumNulls());
  }

  /**
   * Baseline test of metadata cardinality, NDVs and null count.
   * Locks down the values used in later tests to catch external changes
   * easily.
   *
   * Cases:
   * - With stats
   *   - Columns without nulls
   *   - Columns with nulls
   * - Without stats, estimated from file size and schema
   *
   * (The last bit is not yet available.)
   */

  @Test
  public void testMetadata() throws DatabaseNotFoundException, InternalException {
    Catalog catalog = session_.catalog();
    Db db = catalog.getDb("functional");
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(session_.frontend(), "functional", null);
    Set<TableName> tables = Sets.newHashSet(
        new TableName("functional", "alltypes"),
        new TableName("functional", "nullrows"),
        new TableName("functional", "manynulls"));
    mdLoader.loadTables(tables);

    // Table with stats, no nulls
    Table allTypes = db.getTable("alltypes");
    assertEquals(7300, allTypes.getTTableStats().getNum_rows());
    checkColumn(allTypes, "id", 7300, 0);
    checkColumn(allTypes, "bool_col", 2, 0);
    checkColumn(allTypes, "int_col", 10, 0);

    // Table with stats and nulls
    Table nullrows = db.getTable("nullrows");
    assertEquals(26, nullrows.getTTableStats().getNum_rows());
    checkColumn(nullrows, "a", 26, 0);
    // Bug: NDV should be 1 to include nulls
    checkColumn(nullrows, "c", 0, 26);
    checkColumn(nullrows, "f", 6, 0);
    // Enable after data reload
    //checkColumn(nullrows, "g", 7, 20);

    // Table without stats
    Table manynulls = db.getTable("manynulls");
    // Bug: Table cardinality should be guessed from schema & file size.
    assertEquals(-1, manynulls.getTTableStats().getNum_rows());
    checkColumn(manynulls, "id", -1, -1);
  }

  /**
   * Test cardinality of the column references within an AST.
   * Ensures that the metadata cardinality was propagated into the
   * AST, along with possible adjustments.
   *
   * Cases:
   * - With stats
   * - Without stats
   */
  @Test
  public void testColumnCardinality() {
  }

  // Column-level NDV
  // - Normal NDV
  // - Small NDV
  // - Small NDV with nulls
  // - NDV with all nulls

  // Expression selectivity
  // - Test for each expression type
  // - Test for variety of situations
  //   - Valid/invalid table cardinality
  //   - Valid/invalid NDV
  //   - Valid/invalid null count

  @Test
  public void test() {
  }

}
