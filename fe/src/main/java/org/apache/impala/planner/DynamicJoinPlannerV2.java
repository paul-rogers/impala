package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.SingleNodePlanner.JoinPlanner;
import org.apache.impala.planner.SingleNodePlanner.SubplanRef;

import com.google.common.collect.Lists;

public class DynamicJoinPlannerV2 extends JoinPlanner {
  public static class JoinTableIterator implements Iterator<JoinTable>, Iterable<JoinTable> {
    private final List<JoinTable> list_;
    private Candidate candidate_;
    private int posn = -1;
    private int count = 0;

    public JoinTableIterator(List<JoinTable> list, Candidate candidate) {
      list_ = list;
      candidate_ = candidate;
    }

    @Override
    public boolean hasNext() {
      return count == candidate_.unboundTableCount_;
    }

    @Override
    public JoinTable next() {
      for (posn++; !candidate_.mask_[++posn]; posn++) {}
      count++;
      return list_.get(posn);
    }

    @Override
    public Iterator<JoinTable> iterator() {
      return this;
    }
  }

  /**
   * A list of items along with a mask of items removed from the list.
   */
  public static class MaskList<T> implements Iterable<T> {
    private List<T> items_;
    private int size_;
    private boolean[] mask_;

    public MaskList(List<T> items) {
      items_ = items;
      size_ = items_.size();
      mask_ = new boolean[items_.size()];
    }

    public MaskList(MaskList<T> from, int mark) {
      items_ = from.items_;
      size_ = from.size_ - 1;
      mask_ = Arrays.copyOf(from.mask_, from.mask_.length);
      mask_[mark] = true;
    }

    public int size() { return size_; }

    @Override
    public Iterable<T> iterator() {
      return new MaskListIterator<T>(this);
    }
  }

  public static class LeafTableIterator implements Iterator<JoinTable> {
    private final List<JoinTable> list_;
    private final int exclude_;
    private int posn = -1;

    public LeafTableIterator(List<JoinTable> list, int exclude) {
      list_ = list;
      exclude_ = exclude;
    }

    @Override
    public boolean hasNext() {
      if (exclude_ == list_.size()) return posn + 1 == exclude_;
      return posn + 1 == list_.size();
    }

    @Override
    public JoinTable next() {
      if (++posn == exclude_) posn++;
      return list_.get(posn);
    }
  }

  public static class TableIterator<T> implements Iterator<T> {
    private final MaskList<T> list_;
    private int posn = -1;
    private int count = 0;

    public MaskListIterator(MaskList<T> list) {
      list_ = list;
    }

    @Override
    public boolean hasNext() {
      return count == list_.size();
    }

    @Override
    public T next() {
      for (posn++; !list_.mask_[++posn]; posn++) {}
      count++;
      return list_.items_.get(posn);
    }
  }

  public abstract static class Candidate implements Comparable<Candidate> {
    double cost_;
    private boolean[] mask_;
    private int unboundTableCount_;

    public Candidate(double cost,
        boolean[] mask, int unboundTableCount) {
      cost_ = cost;
      mask_ = mask;
      unboundTableCount_ = unboundTableCount;
    }

    public Iterable<JoinTable> iterator(List<JoinTable> tables) {
      return new JoinTableIterator(tables, this);
    }

    @Override
    public int compareTo(Candidate c2) {
      return Double.compare(cost_, c2.cost_);
    }

    public boolean[] subMask(int mark) {
      boolean mask[] = Arrays.copyOf(mask_, mask_.length);
      mask_[mark] = true;
      return mask;
    }

    public abstract PlanNode materialize(DynamicJoinPlannerV2 planner_) throws ImpalaException;
  }

  public static class JoinTable extends Candidate {
    PlanNode node_;
    int id_;
    TableRef tableRef;

    public JoinTable(int id, Pair<TableRef, PlanNode> entry, int tableCount) {
      super(entry.second.getCardinality(),
          makeMask(id, tableCount), tableCount);
      node_ = entry.second;
      id_ = id;
      tableRef = entry.first;
    }

    private static boolean[] makeMask(int id, int tableCount) {
      boolean mask[] = new boolean[tableCount];
      mask[id] = true;
      return mask;
    }

    public int id() { return id_; }

    @Override
    public PlanNode materialize(DynamicJoinPlannerV2 planner_) {
      return node_;
    }
  }

  public static class JoinCandidate extends Candidate {
    Candidate left_;
    JoinNode node_;

    public JoinCandidate(Candidate left, JoinTable right, JoinNode node) {
      super(computeCost(left, node), left.subMask(right.id()), left.unboundTableCount_ - 1);
      left_ = left;
      node_ = node;
    }

    private static double computeCost(Candidate left, PlanNode join) {
      long joinCard = join.getCardinality();
      double cost = left.cost_ + joinCard;
      if (join instanceof NestedLoopJoinNode) {
        if (joinCard > 1) {
          // Large penalty for nested loop join
          cost += 9 * joinCard;
        }
      } else {
        // Penalty for hash table size
        cost += 4 * join.getChild(1).getCardinality();
      }
      return cost;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("")
          .append(String.format("candidate[cost=%.3e ", cost_));
      expand(buf, node_);
      return buf.append("]").toString();
    }

    private void expand(StringBuilder buf, PlanNode node) {
      if (node instanceof JoinNode) {
        buf.append("(");
        expand(buf, node.getChild(0));
        buf.append(") >< (");
        expand(buf, node.getChild(1));
        buf.append(")");
      } else {
        buf.append(node.getDisplayLabelDetail());
      }
    }

     @Override
    public PlanNode materialize(DynamicJoinPlannerV2 planner_) throws ImpalaException {
      left_.materialize(planner_);
      planner_.createSubplan(node_);
      return node_;
    }
  }

  public DynamicJoinPlannerV2(SingleNodePlanner planner, Analyzer analyzer,
      List<Pair<TableRef, PlanNode>> parentRefPlans, List<SubplanRef> subplanRefs) {
    super(planner, analyzer, parentRefPlans, subplanRefs);
  }

  @Override
  public PlanNode plan() throws ImpalaException {
    // Create JoinTables

    List<JoinTable> tables = new ArrayList<>();
    int tableCount = parentRefPlans_.size();
    for (int i = 0; i < parentRefPlans_.size(); i++) {
      tables.add(new JoinTable(i, parentRefPlans_.get(i), tableCount));
    }

    // Pick left-most candidates
    List<Candidate> candidates = chooseLeftMost(tables);

    // Iterate levels from bottom to top
    for (int i = 1; i < tables.size(); i++) {
      candidates = chooseJoin(candidates, tables);
    }

    // Pick best
    return candidates.get(0).materialize(this);
  }

  private List<Candidate> chooseLeftMost(List<JoinTable> tables) {
    List<Candidate> copy = Lists.newArrayList(tables);
    Collections.sort(copy);
    return copy.subList(0, Math.max(3, copy.size()));
  }

  private List<Candidate> chooseJoin(List<Candidate> lefts, List<JoinTable> tables) {
    List<Candidate> candidates = new ArrayList<>();
    for (Candidate left : lefts) {
      for (JoinTable right : left.iterator(tables)) {
        JoinNode join = null;
        candidates.add(new JoinCandidate(left, right, join));
      }
    }
    Collections.sort(candidates);
    return candidates.subList(0, Math.max(3, candidates.size()));
  }
}
