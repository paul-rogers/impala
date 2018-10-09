package com.cloudera.cmf.profile;

import java.util.List;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;

import com.google.common.base.Preconditions;

public class NodeAggregator {

  private TRuntimeProfileNode totals;

  public void add(TRuntimeProfileNode node) {
    if (totals == null) {
      buildTotals(node);
    }
    int n = totals.getCountersSize();
    Preconditions.checkState(n == node.getCountersSize());
    List<TCounter> sums = totals.getCounters();
    List<TCounter> details = node.getCounters();
    for (int i = 0; i < n; i++) {
      TCounter sum = sums.get(i);
      TCounter detail = details.get(i);
      Preconditions.checkState(sum.getName().equals(detail.getName()));
      sum.value += sum.value;
    }
  }

  private void buildTotals(TRuntimeProfileNode node) {
    totals = new TRuntimeProfileNode();
    List<TCounter> source = node.getCounters();
    List<TCounter> dest = totals.getCounters();
    for (TCounter orig : source) {
      TCounter total = new TCounter();
      total.name = orig.name;
      total.unit = orig.unit;
      dest.add(total);
    }
  }

  public TRuntimeProfileNode totals() {
    return totals;
  }
}
