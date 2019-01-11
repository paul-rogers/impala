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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TJoinDistributionMode;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import avro.shaded.com.google.common.base.Objects;

/**
 * Logical join operator. Subclasses correspond to implementations of the join operator
 * (e.g. hash join, nested-loop join, etc).
 */
public abstract class JoinNode extends PlanNode {
  private final static Logger LOG = LoggerFactory.getLogger(JoinNode.class);

  // Default per-instance memory requirement used if no valid stats are available.
  // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
  protected final static long DEFAULT_PER_INSTANCE_MEM = 2L * 1024L * 1024L * 1024L;

  // Slop in percent allowed when comparing stats for the purpose of determining whether
  // an equi-join condition is a foreign/primary key join.
  protected final static double FK_PK_MAX_STATS_DELTA_PERC = 0.05;

  protected JoinOperator joinOp_;

  // Indicates if this join originates from a query block with a straight join hint.
  protected final boolean isStraightJoin_;

  // User-provided hint for the distribution mode. Set to 'NONE' if no hints were given.
  protected final DistributionMode distrModeHint_;

  protected DistributionMode distrMode_ = DistributionMode.NONE;

  // Join conjuncts. eqJoinConjuncts_ are conjuncts of the form <lhs> = <rhs>;
  // otherJoinConjuncts_ are non-equi join conjuncts. For an inner join, join conjuncts
  // are conjuncts from the ON, USING or WHERE clauses. For other join types (e.g. outer
  // and semi joins) these include only conjuncts from the ON and USING clauses.
  protected List<BinaryPredicate> eqJoinConjuncts_;
  protected List<Expr> otherJoinConjuncts_;

  // if valid, the rhs input is materialized outside of this node and is assigned
  // joinTableId_
  protected JoinTableId joinTableId_ = JoinTableId.INVALID;

  public enum DistributionMode {
    NONE("NONE"),
    BROADCAST("BROADCAST"),
    PARTITIONED("PARTITIONED");

    private final String description_;

    private DistributionMode(String description) {
      description_ = description;
    }

    @Override
    public String toString() { return description_; }
    public static DistributionMode fromThrift(TJoinDistributionMode distrMode) {
      if (distrMode == TJoinDistributionMode.BROADCAST) return BROADCAST;
      return PARTITIONED;
    }
  }

  public JoinNode(PlanNode outer, PlanNode inner, boolean isStraightJoin,
      DistributionMode distrMode, JoinOperator joinOp,
      List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherJoinConjuncts,
      String displayName) {
    super(displayName);
    Preconditions.checkNotNull(otherJoinConjuncts);
    isStraightJoin_ = isStraightJoin;
    distrModeHint_ = distrMode;
    joinOp_ = joinOp;
    children_.add(outer);
    children_.add(inner);
    eqJoinConjuncts_ = eqJoinConjuncts;
    otherJoinConjuncts_ = otherJoinConjuncts;
    computeTupleIds();
  }

  @Override
  public void computeTupleIds() {
    Preconditions.checkState(children_.size() == 2);
    clearTupleIds();
    PlanNode outer = children_.get(0);
    PlanNode inner = children_.get(1);

    // Only retain the non-semi-joined tuples of the inputs.
    switch (joinOp_) {
      case LEFT_ANTI_JOIN:
      case LEFT_SEMI_JOIN:
      case NULL_AWARE_LEFT_ANTI_JOIN: {
        tupleIds_.addAll(outer.getTupleIds());
        break;
      }
      case RIGHT_ANTI_JOIN:
      case RIGHT_SEMI_JOIN: {
        tupleIds_.addAll(inner.getTupleIds());
        break;
      }
      default: {
        tupleIds_.addAll(outer.getTupleIds());
        tupleIds_.addAll(inner.getTupleIds());
        break;
      }
    }
    tblRefIds_.addAll(outer.getTblRefIds());
    tblRefIds_.addAll(inner.getTblRefIds());

    // Inherits all the nullable tuple from the children
    // Mark tuples that form the "nullable" side of the outer join as nullable.
    nullableTupleIds_.addAll(inner.getNullableTupleIds());
    nullableTupleIds_.addAll(outer.getNullableTupleIds());
    if (joinOp_.equals(JoinOperator.FULL_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.LEFT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(inner.getTupleIds());
    } else if (joinOp_.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
      nullableTupleIds_.addAll(outer.getTupleIds());
    }
  }

  /**
   * Returns true if the join node can be inverted. Inversions are not allowed
   * in the following cases:
   * 1. Straight join.
   * 2. The operator is a null-aware left anti-join. There is no backend support
   *    for a null-aware right anti-join because we cannot execute it efficiently.
   * 3. In the case of a distributed plan, the resulting join is a non-equi right
   *    semi-join or a non-equi right outer-join. There is no backend support.
   */
  public boolean isInvertible(boolean isLocalPlan) {
    if (isStraightJoin()) return false;
    if (joinOp_.isNullAwareLeftAntiJoin()) return false;
    if (isLocalPlan) return true;
    if (!eqJoinConjuncts_.isEmpty()) return true;
    if (joinOp_.isLeftOuterJoin()) return false;
    if (joinOp_.isLeftSemiJoin()) return false;
    return true;
  }

  public JoinOperator getJoinOp() { return joinOp_; }
  public List<BinaryPredicate> getEqJoinConjuncts() { return eqJoinConjuncts_; }
  public List<Expr> getOtherJoinConjuncts() { return otherJoinConjuncts_; }
  public boolean isStraightJoin() { return isStraightJoin_; }
  public DistributionMode getDistributionModeHint() { return distrModeHint_; }
  public DistributionMode getDistributionMode() { return distrMode_; }
  public void setDistributionMode(DistributionMode distrMode) { distrMode_ = distrMode; }
  public JoinTableId getJoinTableId() { return joinTableId_; }
  public void setJoinTableId(JoinTableId id) { joinTableId_ = id; }
  /// True if this consumes all of its right input before outputting any rows.
  abstract public boolean isBlockingJoinNode();

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    // Do not call super.init() to defer computeStats() until all conjuncts
    // have been collected.
    assignConjuncts(analyzer);
    createDefaultSmap(analyzer);
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
    otherJoinConjuncts_ = Expr.substituteList(otherJoinConjuncts_,
        getCombinedChildSmap(), analyzer, false);
  }

  public List<Expr> boundConjuncts(List<TupleId> tids) {
    List<Expr> matches = new ArrayList<>();
    for (Expr expr : conjuncts_) {
      if (expr.isBoundByTupleIds(tids)) {
        matches.add(expr);
      }
    }
    return matches;
  }

  public static class EquiJoinRef {
     // Cardinality of the base table, |table|
    private double tableCardinality_;
    // NDV of the base table column, |col|
    private double originalNdv_;
    // Selectivity of the input node: the ratio |table'| / |table|
    // Should differ per column; but all columns treated the same for now.
    // (In SELECT * FROM alltypes WHERE int_col = 10
    // We know that int_col has an NDV of 1, but all other columns have
    // independent values and should have a higher NDV.
    // TODO: Revisit this later.
    private double selectivity_;
    // Column NDV after applying filtering adjustments, |col'|
    private double adjustedNdv_;

    private EquiJoinRef(Expr expr) {
      SlotDescriptor slotDesc_ =  expr.findSrcScanSlot();
      if (slotDesc_.getColumn() == null) return;
      // Expression is a column ref. Get stats, which are -1 if unset.
      FeTable tbl = slotDesc_.getParent().getTable();
      tableCardinality_ = tbl.getTTableStats().getNum_rows();
      originalNdv_ = slotDesc_.getStats().getNumDistinctValues();
      adjustedNdv_ = originalNdv_;
      selectivity_ = 1;
      if (tableCardinality_ != -1 && originalNdv_ != -1) {
        adjustedNdv_ = Math.min(adjustedNdv_, tableCardinality_);
      }
    }

    /**
     * Estimate an NDV based on the classic rule:
     * sel(c = x) = 0.1, c = column, x = constant
     * Since sel(c = x) = 1/ndv(c) = 0.1,
     * ndv(c) = |table| * 0.1
     */
    private void updateEstimate(double tableCardinality) {
      if (tableCardinality_ == -1) {
        tableCardinality_ = tableCardinality;
      } else {
        Preconditions.checkState(Math.round(tableCardinality_) == Math.round(tableCardinality));
      }
      if (originalNdv_ == -1)
        originalNdv_ = tableCardinality_ * 0.1;
      // TODO: Better estimate for other items
    }

    private void adjustNdv(double scanCardinality) {
      if (tableCardinality_ == -1) {
        selectivity_ = 1;
        if (originalNdv_ == -1) {
          // If no original NDV and no table cardinality, we have to make
          // up a column NDV. Assume selectivity of .1 or
          // NDV = scanCardinality * 0.1
          adjustedNdv_ = Math.max(1.0, scanCardinality * 0.1);
        } else {
          // We have an original NDV but, oddly, no table NDV.
          // Don't adjust the NDV and hope for the best.
          adjustedNdv_ = Math.min(originalNdv_, scanCardinality);
        }
      } else if (scanCardinality < tableCardinality_){
        selectivity_ = scanCardinality / tableCardinality_;
        // TODO: Per column adjustments based on prior predicates. See comment above.
        // TODO: Use the urn model from the S&S paper.
        adjustedNdv_ = Math.max(1, originalNdv_ * selectivity_);
      }
    }

    private double tableCardinality() { return tableCardinality_; }
    private double adjustedNdv() { return adjustedNdv_; }
  }

  public static class RelationStats {
    private final PlanNode node_;
    private double cardinality_;
    private double selectivity_;
    private double jointKeyCard_ = 1;
    private final List<EquiJoinRef> keys_ = new ArrayList<>();

    private RelationStats(PlanNode node) {
      node_ = node;
      cardinality_ = node.cardinality_;
      selectivity_ = node.selectivity();
      if (selectivity_ == -1) selectivity_ = 1;
    }

    public void addKey(Expr expr) {
      keys_.add(new EquiJoinRef(expr));
    }

    private double cardinality() { return cardinality_; }
    private double selectivity() { return selectivity_; }
    private double jointKeyCardinality() { return jointKeyCard_; }
    private List<TupleId> tupleIds() { return node_.getTupleIds(); }

    /**
     * Try getting cardinality from the left join predicate, if only one.
     * Else, try using the largest column NDV.
     *
     * These are hacks and would be better done in the input node itself.
     * Done here for now to keep changes in one place.
     */
    public void estimateCardinality() {
      if (cardinality_ == 0) { return; }
      if (cardinality_ > 0) {
        for (EquiJoinRef key : keys_) {
          key.adjustNdv(cardinality_);
        }
        return;
      }
      // Try getting cardinality from the left join predicate, if only one.
      if (keys_.size() == 1) {
        cardinality_ = keys_.get(0).tableCardinality();
      }
      // Try using the largest column NDV
      if (cardinality_ == -1) {
        cardinality_ = estimateCardinalityFromNdv();
      }
    }

    /**
     * Estimate node cardinality using the table cardinality, or
     * failing that, the highest NDV. Then, adjust with the node
     * selectivity, if available.
     *
     * TODO: This really should be done in the scan node itself as
     * it is in a better position to handle this case. Done here for now
     * to minimize code change.
     *
     * TODO: Should be based on table size and estimated row width.
     * Using NDV is a work around that works if the table has a unique
     * column (and we have column stats.) Best would be to compare
     * stored and actual row counts, then scale NDVs to account for
     * table growth (or shrinkage).
     *
     * @return estimated node cardinality, or -1 if no estimate can
     * be made.
     */
    private double estimateCardinalityFromNdv() {
      if (!(node_ instanceof ScanNode)) return -1;
      ScanNode scanNode = (ScanNode) node_;
      FeTable table = scanNode.getTupleDesc().getTable();
      if (table == null) return -1;
      long tableCard = table.getTTableStats().getNum_rows();
      if (tableCard == -1) {
        for (Column col : table.getColumns()) {
          tableCard = Math.max(tableCard, col.getStats().getNumDistinctValues());
        }
      }
      // Apply node selectivity, if known.
      if (tableCard == -1) return tableCard;
      if (selectivity_ != -1) tableCard *= selectivity_;
      return tableCard;
    }

    /**
     * If we have only partial information, then guess a selectivity
     * of 1 to keep the calcs simple. Would be better to have worked
     * this out in the child node already than to guess it here.
     */
    private void guessSelectivity() {
      if (selectivity_ == -1) selectivity_ = 1.0;
    }

    // Compute the joint cardinality (NDV) of the join keys assuming
    // the multiplicative rule, and observing that |key| <= |T|.
    // If no join conditions, the largest key cardinality will be 1
    // which turns out to to be what is needed for a Cartesian product.
    private double calcJointKeyCardinality() {
      jointKeyCard_ = 1;
      for (EquiJoinRef key : keys_) {
        // Ignore keys with no data. In the worst case, the
        // joint key cardinality may be 1 (the default), which
        // really should only occur if the table cardinality is 0.
        double keyNdv = key.adjustedNdv();
        if (keyNdv > 0) jointKeyCard_ *= keyNdv;
      }
      jointKeyCard_ = Math.min(jointKeyCard_, cardinality_);
      return jointKeyCard_;
    }

    private void guessCardinality(double cardinality, double selectivity) {
      Preconditions.checkState(cardinality_ == -1);
      cardinality_ = cardinality;
      selectivity_ = selectivity;
      for (EquiJoinRef key : keys_) {
        key.updateEstimate(cardinality_);
      }
    }

    private double filterSelectivity(JoinNode joinNode) {
      return computeCombinedSelectivity(
          joinNode.boundConjuncts(node_.getTupleIds()));
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
      .add("relation", node_.displayName() + " " +
        node_.getDisplayLabelDetail())
      .add("cardinality", cardinality_)
      .add("selectivity", selectivity_)
      .add("key cardinality", jointKeyCard_)
      .toString();
    }
  }

  public static class JoinCalcs {
    private final JoinNode joinNode_;
    private final RelationStats probe_;
    private final RelationStats build_;
    private double largestKeyCard_;
    private final List<Expr> skippedPredicates_ = new ArrayList<>();
    private double cardinality_;
    private double selectivity_ = 1;

    private JoinCalcs(JoinNode node) {
      joinNode_ = node;
      probe_ = new RelationStats(joinNode_.getChild(0));
      build_ = new RelationStats(joinNode_.getChild(1));
    }

    private void calculate() {
      // Create the join conditions. May be partial at this point.
      buildJoinTerms();
      if (!estimateInputCardinalities()) {
        cardinality_ = -1;
        return;
      }
      // If selectivity is unknown, assume 1.0
      probe_.guessSelectivity();
      build_.guessSelectivity();
      calcJointKeyCardinality();
      System.out.println(String.format(
          "  left sel=%.8f, right sel=%.8f, largest key card=%,d",
          probe_.selectivity(),
          build_.selectivity(),
          Math.round(largestKeyCard_)));

      // Calculate the raw join cardinality before additional
      // predicates beyond equi-join.
      cardinality_ = calcBaseCardinality();

      // Apply any non-equi-join conditions
      adjustForExtraPredicates();
      System.out.println(String.format("  --> card=%,d, sel=%.8f",
          Math.round(cardinality_), selectivity_));
    }

    private void buildJoinTerms() {
      System.out.print("  Conjuncts: ");
      for (int i = 0; i < joinNode_.eqJoinConjuncts_.size(); ++i) {
        if (i > 0) System.out.print(", ");
        System.out.print(joinNode_.eqJoinConjuncts_.get(i).toSql());
      }
      System.out.println();
      // Collect join conjuncts that are eligible to participate in cardinality estimation.
      for (Expr eqJoinConjunct: joinNode_.eqJoinConjuncts_) {
        if (!Expr.IS_EQ_BINARY_PREDICATE.apply(eqJoinConjunct)) {
          skippedPredicates_.add(eqJoinConjunct);
        } else {
          probe_.addKey(eqJoinConjunct.getChild(0));
          build_.addKey(eqJoinConjunct.getChild(1));
        }
      }
    }

    private boolean estimateInputCardinalities() {
      probe_.estimateCardinality();
      build_.estimateCardinality();
      double probeCard = probe_.cardinality();
      double buildCard = build_.cardinality();
      if (probeCard == -1 && buildCard == -1) {
        // This is a rather sorry state of affairs. We don't know the size
        // of either table. Should be an error state.
        return false;
      }
      if (probeCard == -1) {
        // Don't know the probe side, but do know the build side.
        // Arbitrarily assume a M:N join with group size of 10.
        // Done to favor true M:1 joins over guesses.
        probe_.guessCardinality(buildCard * 10, build_.selectivity());
      } else if (buildCard == -1) {
        // Symmetrical with above.
        build_.guessCardinality(probeCard * 10, probe_.selectivity());
      }
      return true;
    }

    /**
     * Compute the joint key cardinality (NDV) for compound keys,
     * assuming key independence. If keys are not independent, then
     * their product will generally be larger than the table cardinality,
     * so we cap at the table cardinality and hope that the user intended
     * the combination to be more-or-less unique.
     *
     * Joint key NDV depends on the adjusted NDV of each column. For now,
     * the NDV is adjusted linearly. But, we should track predicates and
     * know that a predicate of the form col = x, where x is constant,
     * reduces the NDV of col to 1. But, if |col| < |table|, then there
     * is room for other columns, with independent values, to have a
     * larger range of NDVs. The urn model tells us how to use probability
     * to estimate the number. But, that all must come later.
     *
     * We also compute the largest of the two key cardinalities for
     * use in join calcs later.
     */
    private void calcJointKeyCardinality() {
      largestKeyCard_ = Math.max(probe_.calcJointKeyCardinality(),
          build_.calcJointKeyCardinality());
    }

    private double calcBaseCardinality() {
      Preconditions.checkState(probe_.cardinality() >= 0);
      Preconditions.checkState(build_.cardinality() >= 0);
      switch (joinNode_.joinOp_) {
      case CROSS_JOIN:
      case INNER_JOIN:
        // Assume the least selective table controls column NDVs
        // Not a great estimate, but all we can do at present.
        selectivity_ = Math.max(probe_.selectivity(), build_.selectivity());
        return calcInnerJoin();
      case FULL_OUTER_JOIN:
        selectivity_ = Math.max(probe_.selectivity(), build_.selectivity());
        return calcFullOuterJoin();
      case LEFT_ANTI_JOIN:
      case NULL_AWARE_LEFT_ANTI_JOIN:
        return calcLeftAntiJoin();
      case LEFT_OUTER_JOIN:
        selectivity_ *= probe_.selectivity();
        return calcLeftOuterJoin();
      case LEFT_SEMI_JOIN:
        return calcLeftSemiJoin();
      case RIGHT_ANTI_JOIN:
        return calcRightAntiJoin();
      case RIGHT_OUTER_JOIN:
        selectivity_ *= build_.selectivity();
        return calcRightOuterJoin();
      case RIGHT_SEMI_JOIN:
        return calcRightSemiJoin();
      default:
        throw new IllegalStateException();
      }
    }

    /**
     * Compute the estimated join cardinality using the expression derived in IMPALA-8014:
     *
     * <pre>
     * |L.k'| = min(sel(L) * prod(|L.ki'|), |L'|)
     *
     * |R.k'| = min(sel(R) * prod(|R.ki'|), |R'|)
     *
     *                  |L'| * |R'|
     * |L' >< R'| = -------------------
     *              max(|L.k'|, |R.k'|)
     *
     * sel(join) = min(sel(L), sel(R))
     * </pre>
     *
     * Where:
     *
     * * L is the left (probe) table
     * * R is the right (build) table
     * * Pki (Bki) is the ith join key on the probe (build) side
     * * sel(L), sel(R) are the selectivities applies on the input nodes
     * * prod() is the product function (capital-pi)
     * * ss is the shared selectivity (which is 1 if no expressions are shared).
     *
     * If the join keys are compound (more than one), we assume key independence
     * and use the multiplicative rule. (See the S&S paper cited below.)
     *
     * Has known limitations: does not properly estimate column NDVs, nor
     * handle join-to-table joins. See IMPALA-8048.
     *
     * @see <a href=
     * "https://pdfs.semanticscholar.org/2735/672262940a23cfce8d4cc1e25c0191de7da7.pdf">
     * S & S Paper</a>.
     */
    private double calcInnerJoin() {
      double probeCard = probe_.cardinality();
      double buildCard = build_.cardinality();
      // Bail out fast in the |build| = 0 case. We may have an EmptySet node.
      // But, the NDV of the build columns still have their values from earlier
      // in the plan; continuing will cause us to work with meaningless numbers.
      if (probeCard == 0 || buildCard == 0) return 0;

      // Apply the cardinality expression
      // Clamp the value to the range (1, MAX_LONG)
      return Math.max(1, probeCard * buildCard / largestKeyCard_);
    }

    /**
     * Like an inner join but:
     *
     * * We assume all right rows appear.
     * * Join cardinality is reduced by any right-side filters reapplied on
     *   this node.
     */
    private double calcRightOuterJoin() {
      return calcOuterJoin(build_, probe_);
    }

    private double calcLeftOuterJoin() {
      return calcOuterJoin(probe_, build_);
    }

    private double calcOuterJoin(RelationStats outer, RelationStats inner) {
      // If no right (build) rows, then |join| is zero
      if (outer.cardinality() == 0) return 0;

      // Outer join is the inner join plus unmatched outer rows.
      // Venn diagram:
      // (outer-only (inner & outer) -inner-only-)
      return calcInnerJoin() + calcOuterOnly(outer, inner);
    }

    private double calcOuterOnly(RelationStats outer, RelationStats inner) {
      // We adopt the Containment assumption from S&S: if the outer
      // has fewer keys than the inner, all the outer keys find a match.
      // If the outer has more keys than the inner, then the outer-only
      // is the ratio of unmatched keys to the total key cardinality,
      // but reduced by the predicates reapplied from the inner child
      // node to the newly generated null columns.
      double outerOnly = outer.cardinality() *
          (1 - inner.jointKeyCardinality() / largestKeyCard_);
      // Reapply inner predicates, which will reduce cardinality
      // below the normally expected outer cardinality. (If we apply foo='bar'
      // to the joined tuples, we'll eliminate all the null values.)
      // The resulting selectivity does not count toward join selectivity as it
      // is already in the probe selectivity.
      //
      // Note that this is, at best, a poor man's solution to the problem.
      // If we have foo = 'bar', we know that it will eliminate all the
      // normally outer rows that would contain null. So the result should be
      // the same as an inner join. If we have foo is null, then it will
      // eliminate none of the outer rows, though it might have eliminated
      // 1/ndv of the inner rows. Bottom line: selectivity should be based
      // on an awareness of the meaning of the predicate, not just the selectivity
      // that made sense during the scan.
      outerOnly *= inner.filterSelectivity(joinNode_);
      return outerOnly;
    }

    private double calcFullOuterJoin() {
      // Venn diagram:
      // (probe only (both) build only)
      return calcOuterOnly(probe_, build_) + calcOuterOnly(build_, probe_) +
        calcInnerJoin();
    }

    /**
     * Returns the estimated cardinality of a semi join node.
     * For a left semi join between child(0) and child(1), we look for equality join
     * conditions "L.c = R.d" (with L being from child(0) and R from child(1)) and use as
     * the cardinality estimate the minimum of
     *   |child(0)| * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     * over all suitable join conditions. The reasoning is that:
     * - each row in child(0) is returned at most once
     * - the probability of a row in child(0) having a match in R is
     *   Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     */
    private double calcLeftSemiJoin() {
      selectivity_ = probe_.selectivity();
      return Math.min(probe_.cardinality(), build_.cardinality());
    }

    private double calcRightSemiJoin() {
      selectivity_ = build_.selectivity();
      return Math.min(probe_.cardinality(), build_.cardinality());
    }

    /**
     * For a left anti join we estimate the cardinality as the minimum of:
     *   |L| * (1 - (smallest NDV) / (largest NDV))
     * over all suitable join conditions. The reasoning is that:
     * - each row in child(0) is returned at most once
     * - if NDV(L.c) > NDV(R.d) then the probability of row in L having a match
     *   in child(1) is (NDV(L.c) - NDV(R.d)) / NDV(L.c)
     */

    private double calcLeftAntiJoin() {
      double unmatched = unmatchedRatio();
      selectivity_ = probe_.selectivity() * unmatched;
      return probe_.cardinality() * unmatched;
    }

    private double calcRightAntiJoin() {
      double unmatched = unmatchedRatio();
      selectivity_ = build_.selectivity() * unmatched;
      return build_.cardinality() * unmatched;
    }

    private double unmatchedRatio() {
      double matched = Math.min(probe_.jointKeyCardinality(),
          build_.jointKeyCardinality()) / largestKeyCard_;
      // Clamp to range (0.1, 1)
      // 0.1 ensures that some rows are emitted since the user expects there
      // to be some. And there can't be more rows than input.
      return Math.max(0.1, Math.min(1.0, 1 - matched));
    }

    /**
     * Adjusts cardinality estimate for non-equi-join predicates, including
     * predicates excluded removed from the equi-join list and those never
     * in the list. Does NOT include any predicates re-applied from child nodes,
     * doing so would cause double accounting for those predicates.
     */
    private void adjustForExtraPredicates() {
      List<Expr> preds = new ArrayList<>();
      preds.addAll(skippedPredicates_);
      // Identify conjuncts bound by only the left or right
      // child. These are conjuncts already evaluated in the
      // child (with possible repeat here for outer join.)
      // We can't consider them for join selectivity.
      for (Expr expr : joinNode_.conjuncts_) {
        if (expr.isBoundByTupleIds(build_.tupleIds())) continue;
        if (expr.isBoundByTupleIds(probe_.tupleIds())) continue;
        preds.add(expr);
      }
      double extraSelectivity = computeCombinedSelectivity(preds);
      selectivity_ *= extraSelectivity;
      cardinality_ *= extraSelectivity;
    }

    private double joinSelectivity() {
      Preconditions.checkState(selectivity_ != -1);
      return selectivity_;
    }

    private long joinCardinality() {
      return Math.round(Math.min(cardinality_, Long.MAX_VALUE));
    }
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    PlanNode probeNode = getChild(0);
    PlanNode buildNode = getChild(1);

    System.out.println(String.format(
        "Join: %s %s card=%,d, sel=%.8f >< %s %s card=%,d sel=%.8f",
        probeNode.displayName(),
        probeNode.getDisplayLabelDetail(),
        probeNode.cardinality_, probeNode.selectivity(),
        buildNode.displayName(),
        buildNode.getDisplayLabelDetail(),
        buildNode.cardinality_, buildNode.selectivity()));

    JoinCalcs calc = new JoinCalcs(this);
    calc.calculate();
    selectivity_ = calc.joinSelectivity();
    cardinality_ = calc.joinCardinality();
    Preconditions.checkState(hasValidStats());
    if (LOG.isTraceEnabled()) {
      LOG.trace("stats Join: cardinality=" + Long.toString(cardinality_));
    }
    System.out.println(String.format("  final sel=%.8f, card=%,d",
        selectivity_, cardinality_));
  }

  /**
   * Inverts the join op, swaps our children, and swaps the children
   * of all eqJoinConjuncts_. All modifications are in place.
   */
  public void invertJoin() {
    joinOp_ = joinOp_.invert();
    Collections.swap(children_, 0, 1);
    for (BinaryPredicate p: eqJoinConjuncts_) p.reverse();
  }

  @Override
  protected String getDisplayLabelDetail() {
    StringBuilder output = new StringBuilder(joinOp_.toString());
    if (distrMode_ != DistributionMode.NONE) output.append(", " + distrMode_.toString());
    return output.toString();
  }

  protected void orderJoinConjunctsByCost() {
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    eqJoinConjuncts_ = orderConjunctsByCost(eqJoinConjuncts_);
    otherJoinConjuncts_ = orderConjunctsByCost(otherJoinConjuncts_);
  }

  @Override
  public ExecPhaseResourceProfiles computeTreeResourceProfiles(
      TQueryOptions queryOptions) {
    Preconditions.checkState(isBlockingJoinNode(), "Only blocking join nodes supported");

    ExecPhaseResourceProfiles buildSideProfile =
        getChild(1).computeTreeResourceProfiles(queryOptions);
    ExecPhaseResourceProfiles probeSideProfile =
        getChild(0).computeTreeResourceProfiles(queryOptions);

    // The peak resource consumption of the build phase is either during the Open() of
    // the build side or while we're doing the join build and calling GetNext() on the
    // build side.
    ResourceProfile buildPhaseProfile = buildSideProfile.duringOpenProfile.max(
        buildSideProfile.postOpenProfile.sum(nodeResourceProfile_));

    ResourceProfile finishedBuildProfile = nodeResourceProfile_;
    if (this instanceof NestedLoopJoinNode) {
      // These exec node implementations may hold references into the build side, which
      // prevents closing of the build side in a timely manner. This means we have to
      // count the post-open resource consumption of the build side in the same way as
      // the other in-memory data structures.
      // TODO: IMPALA-4179: remove this workaround
      finishedBuildProfile = buildSideProfile.postOpenProfile.sum(nodeResourceProfile_);
    }

    // Peak resource consumption of this subtree during Open().
    ResourceProfile duringOpenProfile;
    if (queryOptions.getMt_dop() == 0) {
      // The build and probe side can be open and therefore consume resources
      // simultaneously when mt_dop = 0 because of the async build thread.
      duringOpenProfile = buildPhaseProfile.sum(probeSideProfile.duringOpenProfile);
    } else {
      // Open() of the probe side happens after the build completes.
      duringOpenProfile = buildPhaseProfile.max(
          finishedBuildProfile.sum(probeSideProfile.duringOpenProfile));
    }

    // After Open(), the probe side remains open and the join build remain in memory.
    ResourceProfile probePhaseProfile =
        finishedBuildProfile.sum(probeSideProfile.postOpenProfile);
    return new ExecPhaseResourceProfiles(duringOpenProfile, probePhaseProfile);
  }

  @Override
  public void computePipelineMembership() {
    children_.get(0).computePipelineMembership();
    children_.get(1).computePipelineMembership();
    pipelines_ = new ArrayList<>();
    for (PipelineMembership probePipeline : children_.get(0).getPipelines()) {
      if (probePipeline.getPhase() == TExecNodePhase.GETNEXT) {
          pipelines_.add(new PipelineMembership(
              probePipeline.getId(), probePipeline.getHeight() + 1, TExecNodePhase.GETNEXT));
      }
    }
    for (PipelineMembership buildPipeline : children_.get(1).getPipelines()) {
      if (buildPipeline.getPhase() == TExecNodePhase.GETNEXT) {
        pipelines_.add(new PipelineMembership(
            buildPipeline.getId(), buildPipeline.getHeight() + 1, TExecNodePhase.OPEN));
      }
    }
  }
}
