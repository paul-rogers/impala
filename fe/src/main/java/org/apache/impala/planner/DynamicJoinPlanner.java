package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.SingleNodePlanner.JoinPlanner;
import org.apache.impala.planner.SingleNodePlanner.SubplanRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DynamicJoinPlanner extends JoinPlanner {

  public DynamicJoinPlanner(SingleNodePlanner planner, Analyzer analyzer,
      List<Pair<TableRef, PlanNode>> parentRefPlans, List<SubplanRef> subplanRefs) {
    super(planner, analyzer, parentRefPlans, subplanRefs);
  }

  @Override
  public PlanNode plan() throws ImpalaException {
    System.out.println("createCheapestJoinPlan");
    SingleNodePlanner.LOG.trace("createCheapestJoinPlan");
    if (parentRefPlans_.size() == 1) return parentRefPlans_.get(0).second;

    // collect eligible candidates for the leftmost input; list contains
    // (plan, materialized size)
    List<Pair<TableRef, Long>> candidates = new ArrayList<>();
    for (Pair<TableRef, PlanNode> entry : parentRefPlans_) {
      TableRef ref = entry.first;
      JoinOperator joinOp = ref.getJoinOp();

      // Avoid reordering outer/semi joins which is generally incorrect.
      // TODO: Allow the rhs of any cross join as the leftmost table. This needs careful
      // consideration of the joinOps that result from such a re-ordering (IMPALA-1281).
      if (joinOp.isOuterJoin() || joinOp.isSemiJoin() || joinOp.isCrossJoin()) continue;

      PlanNode plan = entry.second;
      long materializedSize;
      if (plan.getCardinality() == -1) {
        // use 0 for the size to avoid it becoming the leftmost input
        // TODO: Consider raw size of scanned partitions in the absence of stats.
        materializedSize = 0;
      } else {
        Preconditions.checkState(ref.isAnalyzed());
        materializedSize =
            (long) Math.ceil(plan.getAvgRowSize() * (double) plan.getCardinality());
      }
      candidates.add(new Pair<TableRef, Long>(ref, materializedSize));
      if (SingleNodePlanner.LOG.isTraceEnabled()) {
        SingleNodePlanner.LOG.trace(
            "candidate " + ref.getUniqueAlias() + ": " + materializedSize);
      }
      System.out.print("  ");
      System.out.print(ref.getUniqueAlias());
      System.out.print(" - ");
      System.out.print(plan.getDisplayLabel());
      System.out.print(" - ");
      System.out.print(plan.getDisplayLabelDetail());
      System.out.print(", card = ");
      System.out.print(plan.cardinality_);
      System.out.print(", size = ");
      System.out.println(materializedSize);
    }
    if (candidates.isEmpty()) return null;

    // order candidates by descending materialized size; we want to minimize the memory
    // consumption of the materialized hash tables required for the join sequence.
    // We pick the candidate with the largest potential hash table size as the leftmost
    // probe table so that we don't actually materialize the largest table. The other
    // candidates are simply discarded.
    Collections.sort(candidates,
        new Comparator<Pair<TableRef, Long>>() {
          @Override
          public int compare(Pair<TableRef, Long> a, Pair<TableRef, Long> b) {
            return Long.compare(b.second, a.second);
          }
        });

    for (Pair<TableRef, Long> candidate : candidates) {
      PlanNode result = createJoinPlan(candidate.first);
      if (result != null) return result;
    }
    return null;
  }

  public PlanNode createJoinPlan(TableRef left) throws ImpalaException {
    System.out.print("createJoinPlan, leftmost = ");
    System.out.println(left.getUniqueAlias());
    for (Pair<TableRef, PlanNode> refPlan : parentRefPlans_) {
      System.out.print("  ");
      System.out.print(refPlan.first.getUniqueAlias());
      System.out.print(" - ");
      System.out.print(refPlan.second.getDisplayLabel());
      System.out.print(" - ");
      System.out.print(refPlan.second.getDisplayLabelDetail());
      System.out.print(", card = ");
      System.out.println(refPlan.second.cardinality_);
    }
    if (SingleNodePlanner.LOG.isTraceEnabled()) {
      SingleNodePlanner.LOG.trace("createJoinPlan: " + left.getUniqueAlias());
    }
    // the refs that have yet to be joined
    List<Pair<TableRef, PlanNode>> remainingRefs = new ArrayList<>();
    PlanNode root = null;  // root of accumulated join plan
    for (Pair<TableRef, PlanNode> entry : parentRefPlans_) {
      if (entry.first == left) {
        root = entry.second;
      } else {
        remainingRefs.add(entry);
      }
    }
    Preconditions.checkNotNull(root);
    return expandJoinPlan(root, remainingRefs);
  }

  protected PlanNode expandJoinPlan(PlanNode root,
      List<Pair<TableRef, PlanNode>> remainingRefs)
      throws ImpalaException {
    // Split the table refs into partitions split by OUTER or SEMI joins.
    // Each partition has one or more table refs to be joined.
    // OUTER and SEMI partitions have exactly one table ref. This scheme
    // forces a partial ordering based on the presence of these join
    // types.
    List<List<Pair<TableRef, PlanNode>>> joinPartitions = createPartitions(remainingRefs);

    // Each partition can be planned separately. Single-join partitions are trivial
    // to plan. Multi-join partitions require dynamic programming.
    for (int i = joinPartitions.size() - 1; i >= 0; i--) {
      List<Pair<TableRef, PlanNode>> partition = joinPartitions.get(i);
      if (partition.size() == 1) {
        root = createJoin(root, partition.get(0));
      } else {
        root = planSubtree(root, partition);
      }
    }
    return root;
  }

  private List<List<Pair<TableRef, PlanNode>>> createPartitions(List<Pair<TableRef, PlanNode>> remainingRefs) {
    List<List<Pair<TableRef, PlanNode>>> joinPartitions = new ArrayList<>();
    List<Pair<TableRef, PlanNode>> partition = new ArrayList<>();
    for (Pair<TableRef, PlanNode> entry : remainingRefs) {
      TableRef tblRef = entry.first;
      if (tblRef.getJoinOp().isOuterJoin() || tblRef.getJoinOp().isSemiJoin()) {
        if (! partition.isEmpty()) {
          joinPartitions.add(partition);
          partition = new ArrayList<>();
        }
      }
      partition.add(entry);
    }
    if (! partition.isEmpty()) joinPartitions.add(partition);
    return joinPartitions;
  }

  private PlanNode createJoin(PlanNode root, Pair<TableRef, PlanNode> entry) throws ImpalaException {
    PlanNode join = planner_.createJoinNode(root, entry.second, entry.first, analyzer_);
    return createSubplan(join);
  }

  public static class Variations {
    PlanNode right;
    List<DynamicJoinPlanner.Candidate> variations;
  }

  public static class Candidate {
    PlanNode join;
    double cost;

    public Candidate(PlanNode join) {
      this.join = join;
      cost = join.getCardinality();
    }

    public Candidate(DynamicJoinPlanner.Candidate left, PlanNode join) {
      this.join = join;
      long joinCard = join.getCardinality();
      cost = left.cost + joinCard;
      if (join instanceof NestedLoopJoinNode) {
        if (joinCard > 1) {
          // Large penalty for nested loop join
          cost += 9 * joinCard;
        }
      } else {
        // Penalty for hash table size
        cost += 4 * join.getChild(1).getCardinality();
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("")
          .append(String.format("candidate[cost=%.3e ", cost));
      expand(buf, join);
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
  }

  private PlanNode planSubtree(PlanNode left, List<Pair<TableRef, PlanNode>> list) throws ImpalaException {
    List<DynamicJoinPlanner.Candidate> variations = exploreSubtree(left, list);
    DynamicJoinPlanner.Candidate best = variations.get(0);
    System.out.println("planSubtree");
    for (int i = 1; i < variations.size(); i++) {
      DynamicJoinPlanner.Candidate candidate = variations.get(i);
      System.out.print("  ");
      System.out.println(candidate.toString());
      if (candidate.cost < best.cost) best = candidate;
    }
    System.out.println("  Best: " + best.toString());
    return best.join;
  }

  private List<DynamicJoinPlanner.Candidate> exploreSubtree(PlanNode left, List<Pair<TableRef, PlanNode>> list) throws ImpalaException {
    int depth = this.parentRefPlans_.size() - list.size();
    String pad = "";
    for (int i = 0; i < depth; i++) pad += "  ";
    System.out.print(pad + "exploreSubtree: ");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) System.out.print(", ");
      System.out.print(list.get(i).second.getDisplayLabelDetail());
    }
    System.out.println();
    if (list.size() == 1) {
      return Lists.newArrayList(new Candidate(createJoin(left, list.get(0))));
    }

    // Multiple choices at this level. Explore each ordering:
    // ( (subtree-1, c1), (subtree-2, c2), ... (subtree-n, cn) )
    // Where subtree-i is the set of joins for candidates other
    // than ci.
    List<DynamicJoinPlanner.Candidate> candidates = new ArrayList<>();
    for (int i = 0; i < list.size(); i++) {
      Pair<TableRef, PlanNode> right = list.get(i);
      List<Pair<TableRef, PlanNode>> remainder = new ArrayList<>();
      for (int j = 0; j < list.size(); j++) {
        if (j != i) remainder.add(list.get(j));
      }
      List<DynamicJoinPlanner.Candidate> children = exploreSubtree(left, remainder);
      for (DynamicJoinPlanner.Candidate candidate : children) {
        candidates.add(new Candidate(candidate, createJoin(candidate.join, right)));
      }
    }
    sortCandidates(candidates);
    if (candidates.size() <= 3) return candidates;
    return candidates.subList(0, 3);
  }

  private void sortCandidates(List<DynamicJoinPlanner.Candidate> list) {
    Collections.sort(list, new Comparator<DynamicJoinPlanner.Candidate>() {
      @Override
      public int compare(DynamicJoinPlanner.Candidate c1, DynamicJoinPlanner.Candidate c2) {
        return Double.compare(c1.cost, c2.cost);
      }
    });
  }

}