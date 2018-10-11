package com.cloudera.cmf.profile;

import java.util.List;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TUnit;

import com.google.common.base.Preconditions;

public class NodeAggregator {

  private TRuntimeProfileNode totals;

  public NodeAggregator(TRuntimeProfileNode node) {
    this.totals = node;
  }

  public void add(TRuntimeProfileNode node) {
    int n = totals.getCountersSize();
    if (n == 0) {
      buildTotals(node);
      n = totals.getCountersSize();
    }
    Preconditions.checkState(n == node.getCountersSize());
    List<TCounter> sums = totals.getCounters();
    List<TCounter> details = node.getCounters();
    for (int i = 0; i < n; i++) {
      TCounter sum = sums.get(i);
      TCounter detail = details.get(i);
      Preconditions.checkState(sum.getName().equals(detail.getName()));
      if (detail.getUnit() == TUnit.DOUBLE_VALUE) {
        sum.value = Double.doubleToLongBits(
                      Double.longBitsToDouble(sum.value) +
                      Double.longBitsToDouble(detail.value));
      } else {
        sum.value += detail.value;
      }
    }
  }

  private void buildTotals(TRuntimeProfileNode node) {
    List<TCounter> source = node.getCounters();
    List<TCounter> dest = totals.getCounters();
    for (TCounter orig : source) {
      TCounter total = new TCounter();
      total.setName(orig.name);
      total.setUnit(orig.unit);
      total.setValue(0);
      dest.add(total);
    }
  }

  public TRuntimeProfileNode totals() {
    return totals;
  }
}
