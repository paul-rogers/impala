package org.apache.impala.planner;

import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.JoinNode.EqJoinConjunctScanSlots;

public abstract class PlanCalcs {

  public static class FilterCalcs {
    List<Expr> preds;
    double effectiveSelectivity;
    long outputRowCount;

    public void serialize(StringBuilder buf, String indent) {
      for (int i = 0; i < preds.size(); i++) {
        Expr expr = preds.get(i);
        buf.append(indent).append("expr ").append(i+1)
          .append(": ").append(expr.toSql()).append("\n");
        buf.append(indent).append("selectivity ").append(i+1)
          .append(": ").append(String.format("%.3e", expr.getSelectivity()))
          .append("\n");
      }
      buf.append(indent).append("net selectivity: ")
        .append(String.format("%.3e", effectiveSelectivity)).append("\n");
      buf.append(indent).append("output cardinality: ").append(outputRowCount);
    }
  }

  public static class PartitionFilterCalcs extends FilterCalcs {
    long partitionCount;
    long partitionMatchCount;
  }

  public abstract static class ScanCalcs extends PlanCalcs {
    long tableCardinality;
    FilterCalcs projectionFilter;

    public void serializeHead(StringBuilder buf, String indent) {
      buf.append(indent).append("table cardinality: ").append(node.getTable().getFullName());
    }

    @Override
    public void serializeTail(StringBuilder buf, String indent) {
      if (projectionFilter != null) projectionFilter.serialize(buf, indent + "  ");
      super.serializeTail(buf, indent);
    }
  }

  public static class HdfsScanCalcs extends ScanCalcs {
    HdfsScanNode node;
    FilterCalcs partitionFilter;

    @Override
    public void serialize(StringBuilder buf, String indent) {
      buf.append(indent).append("table: ").append(node.getTable().getFullName());
      serializeHead(buf, indent);
      serializeTail(buf, indent);
    }
  }

  public static class KeyCalcs {
    EqJoinConjunctScanSlots key;
    long leftEffectiveCard;
    long rightEffectiveCard;

    public void serialize(StringBuilder buf, String indent) {
      buf.append(indent).append("expr: ").append(key.toString());
      buf.append(indent).append("build col: ").append(key.conjunct().getChild(1).toSql());
      buf.append(indent).append("build ndv: ").append(key.rhsNdv());
      buf.append(indent).append("build rows: ").append(key.rhsNumRows());
      buf.append(indent).append("build est. ndv: ").append(rightEffectiveCard);
      buf.append(indent).append("probe col: ").append(key.conjunct().getChild(0).toSql());
      buf.append(indent).append("probe ndv: ").append(key.lhsNdv());
      buf.append(indent).append("probe rows: ").append(key.lhsNumRows());
      buf.append(indent).append("probe est. ndv: ").append(leftEffectiveCard);
    }
  }

  public static class JoinCalcs extends PlanCalcs {
    PlanCalcs left;
    PlanCalcs right;
    List<Pair<KeyCalcs, KeyCalcs>> keys;

    @Override
    public void serialize(StringBuilder buf, String indent) {
      String childIndent = indent + "  ";
      buf.append(indent).append("table: ").append(node.getTable().getFullName());
      for (int i = 0; i < keys.size(); i++) {
        buf.append(indent).append("key ").append(i+1).append(":\n");
        keys.get(i).serialize(buf, childIndent);
      }
      serializeTail(buf, indent);
      buf.append(indent).append("build:\n");
      right.serialize(buf, childIndent);
      buf.append(indent).append("probe:\n");
      left.serialize(buf, childIndent);
    }
  }

  long outputCardinality;
  double effectiveSelectivity;

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    serialize(buf, "");
    return buf.toString();
  }

  public abstract void serialize(StringBuilder buf, String indent);

  public void serializeTail(StringBuilder buf, String indent) {
    buf.append("output rows: ").append(outputCardinality).append("\n");
    buf.append("effective selectivity: ").append(
        String.format("%.3e", effectiveSelectivity)).append("\n");
  }
}
