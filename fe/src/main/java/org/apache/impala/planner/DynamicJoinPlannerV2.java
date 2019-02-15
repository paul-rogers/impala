package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.shaded.com.google.common.base.Preconditions;
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
      return count < candidate_.unboundTableCount_;
    }

    @Override
    public JoinTable next() {
      for (posn++; candidate_.mask_[posn]; posn++) {}
      count++;
      return list_.get(posn);
    }

    @Override
    public Iterator<JoinTable> iterator() {
      return this;
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
      mask[mark] = true;
      return mask;
    }

    public abstract PlanNode node();
    public abstract PlanNode materialize(DynamicJoinPlannerV2 planner_) throws ImpalaException;
  }

  public static class JoinTable extends Candidate {
    PlanNode node_;
    int id_;
    TableRef tableRef;

    public JoinTable(int id, Pair<TableRef, PlanNode> entry, int tableCount) {
      super(entry.second.getCardinality(),
          makeMask(id, tableCount), tableCount - 1);
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

    @Override
    public PlanNode node() { return node_; }
    public TableRef tableRef() { return tableRef; }

    @Override
    public String toString() { return node_.toString(); }
  }

  public static class JoinCandidate extends Candidate {
    Candidate left_;
    PlanNode node_;

    public JoinCandidate(Candidate left, JoinTable right, PlanNode node) {
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

    @Override
    public PlanNode node() { return node_; }
  }

  public DynamicJoinPlannerV2(SingleNodePlanner planner, Analyzer analyzer,
      List<Pair<TableRef, PlanNode>> parentRefPlans, List<SubplanRef> subplanRefs) {
    super(planner, analyzer, parentRefPlans, subplanRefs);
  }

  @Override
  public PlanNode plan() throws ImpalaException {
    if (parentRefPlans_.size() == 1) return parentRefPlans_.get(0).second;

    // Create Join Tables
    List<JoinTable> tables = new ArrayList<>();
    int tableCount = parentRefPlans_.size();
    for (int i = 0; i < parentRefPlans_.size(); i++) {
      tables.add(new JoinTable(i, parentRefPlans_.get(i), tableCount));
    }

    // Pick left-most candidates
    List<Candidate> candidates = chooseLeftMost(tables);

    // Iterate levels from bottom to top
    for (int i = 1; i < tables.size(); i++) {
      System.out.println("  Step: " + i);
      candidates = chooseJoin(candidates, tables);
    }

    // Pick best
    PlanNode root = candidates.get(0).materialize(this);
    analyzer_.setAssignedConjuncts(root.getAssignedConjuncts());
    return root;
  }

  private List<Candidate> chooseLeftMost(List<JoinTable> tables) {
    List<Candidate> copy = Lists.newArrayList(tables);
    Collections.sort(copy);
    Collections.reverse(copy);
    return copy.subList(0, Math.min(3, copy.size()));
  }

  private List<Candidate> chooseJoin(List<Candidate> lefts, List<JoinTable> tables) throws ImpalaException {
    List<Candidate> candidates = new ArrayList<>();
    for (Candidate left : lefts) {
      System.out.println("    Left: " + left.toString());
      for (JoinTable right : left.iterator(tables)) {
        Preconditions.checkNotNull(right);
        System.out.println("    Right: " + right.toString());
        analyzer_.setAssignedConjuncts(left.node().getAssignedConjuncts());
        PlanNode join = planner_.createJoinNode(left.node(), right.node(), right.tableRef(), analyzer_);
        if (join != null) {
          candidates.add(new JoinCandidate(left, right, join));
        }
      }
    }
    Collections.sort(candidates);
    return candidates.subList(0, Math.min(3, candidates.size()));
  }
}
