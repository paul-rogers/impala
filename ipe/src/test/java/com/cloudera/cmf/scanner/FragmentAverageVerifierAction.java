package com.cloudera.cmf.scanner;

import java.util.List;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TUnit;

import com.cloudera.cmf.profile.OperatorPNode;
import com.cloudera.cmf.profile.ParseUtils;
import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.profile.QueryDAG;
import com.cloudera.cmf.profile.QueryDAG.OperatorSynNode;
import com.cloudera.cmf.scanner.ProfileScanner.AbstractAction;

import jersey.repackaged.com.google.common.base.Preconditions;

/**
 * Debug/test rule to verify that the average value in the fragment
 * average subtree really is the average of the details in the
 * fragment instances subtree.
 */
public class FragmentAverageVerifierAction extends AbstractAction {

  @Override
  public void apply(ProfileFacade profile) {
    QueryDAG dag = profile.dag();
    for (OperatorSynNode opSyn : dag.operators()) {
      verifyOperator(opSyn);
    }
  }

  private void verifyOperator(OperatorSynNode opSyn) {
    // Root is the fake PLAN_ROOT
    if (opSyn.isRoot()) { return; }
    // Also ignore the EXCHANGE just under the root
    if (opSyn.parent().planNode().isRoot()) { return; }
    OperatorPNode avgNode = opSyn.avgNode();
    int n = opSyn.instanceCount();
    OperatorPNode totals = opSyn.totals();
    List<TCounter> avgCounters = avgNode.node().getCounters();
    List<TCounter> totalCounters = totals.node().getCounters();
    int cn = totalCounters.size();
    Preconditions.checkState(avgCounters.size() == cn);
    for (int i = 0; i < cn;  i++) {
      TCounter avgCounter = avgCounters.get(i);
      TCounter totalCounter = totalCounters.get(i);
      if (avgCounter.getUnit() == TUnit.DOUBLE_VALUE) {
        double avgDouble = ParseUtils.getDoubleCounter(avgCounter);
        double actualDouble = ParseUtils.getDoubleCounter(totalCounter) / n;
        if (avgDouble < 0.0001 && actualDouble < 0.0001) { continue; }
        double delta = avgDouble - actualDouble;
        double ratio = delta / actualDouble;
        if (ratio > 0.05) {
          fmt.attrib(opSyn.title(),
              String.format(
                  "Incorrect average for %s (%s), " +
                  "expected = %f, actual = %f",
                  avgCounter.getName(),
                  avgCounter.getUnit().name(),
                  avgDouble, actualDouble));
        }
      } else {
        long avg = totalCounter.getValue() / n;
        long actual = avgCounter.getValue();
        long delta = Math.abs(avg - actual);
        if (delta > 1) {
          fmt.attrib(opSyn.title(),
              String.format(
                  "Incorrect average for %s (%s), " +
                  "expected = %d, actual = %d",
                  avgCounter.getName(),
                  avgCounter.getUnit().name(),
                  avg, actual));
        }
      }
    }
  }
}
