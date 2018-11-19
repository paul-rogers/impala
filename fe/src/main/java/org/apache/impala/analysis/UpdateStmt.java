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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Representation of an Update statement.
 *
 * Example UPDATE statement:
 *
 *     UPDATE target_table
 *       SET slotRef=expr, [slotRef=expr, ...]
 *       FROM table_ref_list
 *       WHERE conjunct_list
 *
 * An update statement consists of four major parts. First, the target table path,
 * second, the list of assignments, the optional FROM clause, and the optional where
 * clause. The type of the right-hand side of each assignments must be
 * assignment compatible with the left-hand side column type.
 *
 * Currently, only Kudu tables can be updated.
 */
public class UpdateStmt extends ModifyStmt {
  // List of explicitly mentioned assignment expressions in the UPDATE's SET clause
  protected final List<Pair<SlotRef, Expr>> assignments_;

  public UpdateStmt(List<String> targetTablePath, FromClause tableRefs,
      List<Pair<SlotRef, Expr>> assignmentExprs, Expr wherePredicate) {
    super(targetTablePath, tableRefs, wherePredicate);
    assignments_ = Preconditions.checkNotNull(assignmentExprs);
  }

  public UpdateStmt(UpdateStmt other) {
    super(other.targetTablePath_, other.fromClause_.clone(),
        other.wherePredicate_);
    assignments_ = Preconditions.checkNotNull(new ArrayList<>());
  }

  /**
   * Return an instance of a KuduTableSink specialized as an Update operation.
   */
  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table_ != null);
    DataSink dataSink = TableSink.create(table_, TableSink.Op.UPDATE,
        ImmutableList.<Expr>of(), referencedColumns_, false, false,
        ImmutableList.<Integer>of());
    Preconditions.checkState(!referencedColumns_.isEmpty());
    return dataSink;
  }

  /**
   * Builds and validates the sourceStmt_. The select list of the sourceStmt_ contains
   * first the SlotRefs for the key Columns, followed by the expressions representing the
   * assignments. This method sets the member variables for the sourceStmt_ and the
   * referencedColumns_.
   *
   * This is only run once, on the first analysis. Following analysis will reset() and
   * reuse previously created statements.
   */
  @Override
  protected void createSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    super.createSourceStmt(analyzer);

    // cast result expressions to the correct type of the referenced slot of the
    // target table
    int keyColumnsOffset = table_.getPrimaryKeyColumnNames().size();
    for (int i = keyColumnsOffset; i < sourceStmt_.resultExprs_.size(); ++i) {
      sourceStmt_.resultExprs_.set(i, sourceStmt_.resultExprs_.get(i).castTo(
          assignments_.get(i - keyColumnsOffset).first.getType()));
    }
  }

  /**
   * Validates the list of value assignments that should be used to modify the target
   * table. It verifies that only those columns are referenced that belong to the target
   * table, no key columns are modified, and that a single column is not modified multiple
   * times. Analyzes the Exprs and SlotRefs of assignments_ and writes a list of
   * SelectListItems to the out parameter selectList that is used to build the select list
   * for sourceStmt_. A list of integers indicating the column position of an entry in the
   * select list in the target table is written to the out parameter referencedColumns.
   *
   * In addition to the expressions that are generated for each assignment, the
   * expression list contains an expression for each key column. The key columns
   * are always prepended to the list of expression representing the assignments.
   */
  @Override
  protected void buildAndValidateAssignmentExprs(Analyzer analyzer,
      List<SelectListItem> selectList)
      throws AnalysisException {
    referencedColumns_ = Lists.newArrayList();
    // The order of the referenced columns equals the order of the result expressions
    Set<SlotId> uniqueSlots = new HashSet<>();
    Set<SlotId> keySlots = new HashSet<>();

    // Mapping from column name to index
    List<Column> cols = table_.getColumnsInHiveOrder();
    Map<String, Integer> colIndexMap = new HashMap<>();
    for (int i = 0; i < cols.size(); i++) {
      colIndexMap.put(cols.get(i).getName(), i);
    }

    // Add the key columns as slot refs
    for (String k : table_.getPrimaryKeyColumnNames()) {
      ArrayList<String> path = Path.createRawPath(targetTableRef_.getUniqueAlias(), k);
      SlotRef ref = new SlotRef(path);
      ref.analyze(analyzer);
      selectList.add(new SelectListItem(ref, null));
      uniqueSlots.add(ref.getSlotId());
      keySlots.add(ref.getSlotId());
      referencedColumns_.add(colIndexMap.get(k));
    }

    // Assignments are only used in the context of updates.
    for (Pair<SlotRef, Expr> valueAssignment : assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      lhsSlotRef.analyze(analyzer);

      Expr rhsExpr = valueAssignment.second;
      // No subqueries for rhs expression
      if (rhsExpr.contains(Subquery.class)) {
        throw new AnalysisException(
            format("Subqueries are not supported as update expressions for column '%s'",
                lhsSlotRef.toSql()));
      }
      rhsExpr.analyze(analyzer);

      // Correct target table
      if (!lhsSlotRef.isBoundByTupleIds(targetTableRef_.getId().asList())) {
        throw new AnalysisException(
            format("Left-hand side column '%s' in assignment expression '%s=%s' does not "
                + "belong to target table '%s'", lhsSlotRef.toSql(), lhsSlotRef.toSql(),
                rhsExpr.toSql(), targetTableRef_.getDesc().getTable().getFullName()));
      }

      Column c = lhsSlotRef.getResolvedPath().destColumn();
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      if (c == null) {
        throw new AnalysisException(
            format("Left-hand side in assignment expression '%s=%s' must be a column " +
                "reference", lhsSlotRef.toSql(), rhsExpr.toSql()));
      }

      if (keySlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(format("Key column '%s' cannot be updated.",
            lhsSlotRef.toSql()));
      }

      if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(
            format("Duplicate value assignment to column: '%s'", lhsSlotRef.toSql()));
      }

      rhsExpr = checkTypeCompatibility(targetTableRef_.getDesc().getTable().getFullName(),
          c, rhsExpr, analyzer.isDecimalV2());
      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns_.add(colIndexMap.get(c.getName()));
    }
  }

  @Override
  public UpdateStmt clone() {
    return new UpdateStmt(this);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    for (Pair<SlotRef, Expr> assignment: assignments_) {
      assignment.second = rewriter.rewrite(assignment.second, analyzer_);
    }
    super.rewriteExprs(rewriter);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (!options.showRewritten() && sqlString_ != null) return sqlString_;

    StringBuilder b = new StringBuilder();
    b.append("UPDATE ");

    if (fromClause_ == null) {
      b.append(targetTableRef_.toSql(options));
    } else {
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql(options));
      }
    }
    b.append(" SET");

    boolean first = true;
    for (Pair<SlotRef, Expr> i : assignments_) {
      if (!first) {
        b.append(",");
      } else {
        first = false;
      }
      b.append(format(" %s = %s", i.first.toSql(options), i.second.toSql(options)));
    }

    b.append(fromClause_.toSql(options));

    if (wherePredicate_ != null) {
      b.append(" WHERE ");
      b.append(wherePredicate_.toSql(options));
    }
    return b.toString();
  }
}
