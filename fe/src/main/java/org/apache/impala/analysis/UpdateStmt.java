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
import java.util.List;

import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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

  public static class UpdateAssignment {
    protected final SlotRef lhs_;
    protected Expr rhs_;
    protected String originalRhs_;

    public UpdateAssignment(Pair<SlotRef, Expr> pair) {
      lhs_ = pair.first;
      rhs_ = pair.second;
    }

    public UpdateAssignment(UpdateAssignment from) {
      this.lhs_ = (SlotRef) from.lhs_.clone();
      this.rhs_ = from.rhs_.clone();
      this.originalRhs_ = from.originalRhs_;
    }

    public UpdateAssignment clone() {
      return new UpdateAssignment(this);
    }
  }

  // List of explicitly mentioned assignment expressions in the UPDATE's SET clause
  protected final List<UpdateAssignment> assignments_ = new ArrayList<>();

  public UpdateStmt(List<String> targetTablePath, FromClause tableRefs,
      List<Pair<SlotRef, Expr>> assignmentExprs, Expr wherePredicate) {
    super(targetTablePath, tableRefs, wherePredicate);
    Preconditions.checkNotNull(assignmentExprs);
    for (Pair<SlotRef, Expr> pair : assignmentExprs) {
      assignments_.add(new UpdateAssignment(pair));
    }
  }

  public UpdateStmt(UpdateStmt other) {
    super(other.targetTablePath_, other.fromClause_.clone(),
        other.whereClause_);
    for (UpdateAssignment a : other.assignments_) {
      assignments_.add(a.clone());
    }
  }

  public List<UpdateAssignment> getAssignments() { return assignments_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    UpdateAnalyzer updateAnalyzer = new UpdateAnalyzer(analyzer);
    updateAnalyzer.analyze();
  }

  public class UpdateAnalyzer extends ModifyAnalyzer {

    protected UpdateAnalyzer(Analyzer analyzer) {
      super(analyzer);
    }

    @Override
    protected void createSourceStmt() throws AnalysisException {
      super.createSourceStmt();

      // cast result expressions to the correct type of the referenced slot of the
      // target table
      int keyColumnsOffset = table_.getPrimaryKeyColumnNames().size();
      for (int i = keyColumnsOffset; i < sourceStmt_.resultExprs_.size(); ++i) {
        sourceStmt_.resultExprs_.set(i, sourceStmt_.resultExprs_.get(i).castTo(
            assignments_.get(i - keyColumnsOffset).lhs_.getType()));
      }
    }

    protected void buildAndValidateAssignmentExprs(
        List<SelectListItem> selectList, List<Integer> referencedColumns)
        throws AnalysisException {
      super.buildAndValidateAssignmentExprs(selectList, referencedColumns);

      // Assignments are only used in the context of updates.
      for (UpdateAssignment valueAssignment : assignments_) {
        SlotRef lhsSlotRef = valueAssignment.lhs_;
        lhsSlotRef.analyze(analyzer);

        Expr rhsExpr = valueAssignment.rhs_;
        // No subqueries for rhs expression
        if (rhsExpr.contains(Subquery.class)) {
          throw new AnalysisException(
              format("Subqueries are not supported as update expressions:\n" +
                  "%s = %s",
                  lhsSlotRef.toSql(), rhsExpr.toSql()));
        }
        rhsExpr.analyze(analyzer);
        valueAssignment.originalRhs_ = rhsExpr.toSql();

        // Correct target table
        // Note: testing shows that this never triggers.
        // Caught as an invalid slot reference in resolve above.
        if (!lhsSlotRef.isBoundByTupleIds(targetTableRef_.getId().asList())) {
          throw new AnalysisException(
              format("Left-hand side of assignment does not "
                  + "belong to target table:\n" +
                  "Expression: %s = %s\nTarget table: %s",
                  lhsSlotRef.toSql(), valueAssignment.originalRhs_,
                  targetTableRef_.getDesc().getTable().getFullName()));
        }

        // Note: This does not seem reachable either. If the expression
        // is a non-symbol, the parser fails. If it is a non-column,
        // analysis fails above.
        Column c = lhsSlotRef.getResolvedPath().destColumn();
        // TODO(Kudu) Add test for this code-path when Kudu supports nested types
        if (c == null) {
          throw new AnalysisException(
              format("Left-hand side of assignment must be a column " +
                  "reference: %s = %s", lhsSlotRef.toSql(),
                  valueAssignment.originalRhs_));
        }

        if (keySlots.contains(lhsSlotRef.getSlotId())) {
          throw new AnalysisException(format("Key column cannot be updated: %s",
              lhsSlotRef.toSql()));
        }

        if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
          throw new AnalysisException(
              format("Duplicate value assignment to column: %s",
                  lhsSlotRef.toSql()));
        }

        rhsExpr = checkTypeCompatibility(
            targetTableRef_.getDesc().getTable().getFullName(),
            c, rhsExpr, analyzer.isDecimalV2());

        // Rewrite rhs after error checks
        rhsExpr = analyzer.rewrite(rhsExpr);
        valueAssignment.rhs_ = rhsExpr;
        uniqueSlots.add(lhsSlotRef.getSlotId());
        selectList.add(new SelectListItem(rhsExpr, null));
        referencedColumns.add(colIndexMap.get(c.getName()));
      }
    }
  }

  @Deprecated
  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    super.rewriteExprs(rewriter);
    for (UpdateAssignment assignment: assignments_) {
      assignment.rhs_ = rewriter.rewrite(assignment.rhs_, analyzer_);
    }
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

  @Override
  public UpdateStmt clone() {
    return new UpdateStmt(this);
  }

  @Override
  public String toSql(boolean rewritten) {
    StringBuilder b = new StringBuilder();
    b.append("UPDATE ");

    if (fromClause_ == null) {
      b.append(targetTableRef_.toSql(rewritten));
    } else {
      if (targetTableRef_.hasExplicitAlias()) {
        b.append(targetTableRef_.getExplicitAlias());
      } else {
        b.append(targetTableRef_.toSql(rewritten));
      }
    }
    b.append(" SET ");

    for (int i = 0; i < assignments_.size(); i++) {
      UpdateAssignment a = assignments_.get(i);
      if (i > 0) {
        b.append(", ");
      }
      b.append(a.lhs_.toSql())
       .append(" = ")
       .append(rewritten
           ? a.rhs_.toSql()
           : a.originalRhs_);
    }

    b.append(whereToSql(rewritten));
    return b.toString();
  }
}
