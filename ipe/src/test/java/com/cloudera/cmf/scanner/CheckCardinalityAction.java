package com.cloudera.cmf.scanner;

import com.cloudera.cmf.printer.AttribBufFormatter;
import com.cloudera.cmf.printer.TableBuilder;
import com.cloudera.cmf.printer.TableBuilder.Justification;
import com.cloudera.cmf.printer.TablePrinter;
import com.cloudera.cmf.profile.Attrib;
import com.cloudera.cmf.profile.OperatorPNode;
import com.cloudera.cmf.profile.ParseUtils;
import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.profile.QueryDAG;
import com.cloudera.cmf.profile.QueryDAG.OperatorSynNode;
import com.cloudera.cmf.profile.QueryPlan.AggregateNode;
import com.cloudera.cmf.profile.QueryPlan.AggregateNode.AggType;
import com.cloudera.cmf.profile.QueryPlan.HdfsScanNode;
import com.cloudera.cmf.scanner.ProfileScanner.AbstractAction;

public class CheckCardinalityAction extends AbstractAction {

  public enum FlowNodeType {
    SCAN, AGG, JOIN, ROOT, FILTER, SOURCE
  }

  public static class FlowNode {
    private FlowNodeType type;
    private OperatorSynNode node;
    private String title;
    private long expectedCard;
    private long actualCard;
    private double expectedSelectivity;
    private double actualSelectivity;
    private FlowNode left;
    private FlowNode right;
    private String notes;

    public FlowNode(OperatorSynNode opSyn, FlowNodeType type, String title) {
      node = opSyn;
      this.type = type;
      this.title = String.format("%d: %s",
          opSyn.operatorId(), title);
    }

    public FlowNode(FlowNodeType type, String label) {
      this.type = type;
      title = label;
    }

    public void addNote(String note) {
      if (notes == null) {
        notes = note;
      } else {
        notes += "\n" + note;
      }
    }

    public void computeSelectivity() {
      double input = left.expectedCard + (right == null ? 0 : right.expectedCard);
      expectedSelectivity = (input == 0) ? 0 : expectedCard / input;
      input = left.actualCard + (right == null ? 0 : right.actualCard);
      actualSelectivity = (input == 0) ? 0 : actualCard / input;

      double expected = (expectedCard == 0) ? 1 : expectedCard;
      double ratio = actualCard / expected;
      double error = Math.log10(ratio);
      if (error > 1) {
        if (ratio < 1000) {
          addNote("Off by " + ((int) ratio) + "x");
        } else {
          addNote("Off by 10^" + ((int) error));
        }
      }
    }

    @Override
    public String toString() {
      AttribBufFormatter fmt = new AttribBufFormatter();
      format(fmt);
      return fmt.toString();
    }

    private void format(AttribBufFormatter fmt) {
      fmt.startGroup(title);
//      fmt.attrib("Type", type.name());
      fmt.attrib("Expected", expectedCard);
      fmt.attrib("Actual", actualCard);
//      if (left != null) {
//        fmt.attrib("Expected Reduction", String.format("%.5f", expectedSelectivity));
//        fmt.attrib("Actual Reduction", String.format("%.5f", actualSelectivity));
//      }
      if (left != null) { left.format(fmt); }
      if (right != null) { right.format(fmt); }
      fmt.endGroup();
    }
  }

  private FlowNode root;

  @Override
  public void apply(ProfileFacade profile) {
    QueryDAG dag = profile.dag();

    OperatorSynNode rootNode = dag.rootOperator();
    root = new FlowNode(rootNode, FlowNodeType.ROOT, "root");
    root.expectedCard = rootNode.planNode().estCardinality();
    root.actualCard = dag.rootFragment().coord().counter(Attrib.FragmentCounter.ROWS_PRODUCED);
    root.left = expand(rootNode.child());
    System.out.print(root.toString());

    TableBuilder table = new TableBuilder()
        .startHeader()
          .cell("Node")
          .startCell()
            .format("%,d")
            .justify(Justification.RIGHT)
            .value("Plan\nCardinality")
          .endCell()
          .startCell()
            .format("%,d")
            .justify(Justification.RIGHT)
            .value("Actual\nCardinality")
          .endCell()
          .cell("Notes")
         .endRow();
    formatTable(0, table, root, "");
    TablePrinter printer = new TablePrinter(table);
    printer.print();

    System.out.println(joinExpr("", root));
  }

  private String joinExpr(String indent, FlowNode node) {
    String nest = indent + "  ";
    switch (node.type) {
    case JOIN:
      return "join-" + node.node.operatorId() +
          "(\n" + nest + "build = " +
        ParseUtils.format(node.left.actualCard) +
        ": " + joinExpr(nest, node.left) +
        ",\n" + nest + "probe = " +
        ParseUtils.format(node.right.actualCard) +
        ": " + joinExpr(nest, node.right) + ")";
    case SCAN:
      return "scan-" + node.node.operatorId() +
        "=(" + ParseUtils.format(node.left.actualCard) + ")";
    default:
      return joinExpr(indent, node.left);
    }
  }

  private void formatTable(int depth, TableBuilder table, FlowNode node, String tag) {
    String indent = TablePrinter.repeat(" ", depth);
    table
      .startRow()
        .cell(indent + node.title + (tag.isEmpty() ? tag : " " + tag))
        .cell(node.expectedCard)
        .cell(node.actualCard)
        .cell(node.notes == null ? "" : node.notes)
      .endRow();
   if (node.right != null) {
      formatTable(depth + 1, table, node.left, "");
      formatTable(depth + 1, table, node.right, "");
   } else if (node.left != null) {
     formatTable(depth + 1, table, node.left, "");
   }
  }

  private FlowNode expand(OperatorSynNode opNode) {
    switch (opNode.opType()) {
    case AGG:
      return expandAgg(opNode);
    case EXCHANGE:
      return expand(opNode.child());
    case HASH_JOIN:
      return expandJoin(opNode);
    case HDFS_SCAN:
      return expandScan(opNode);
    default:
      throw new IllegalStateException();
    }
  }

  private FlowNode expandAgg(OperatorSynNode aggNode) {
    if (((AggregateNode) aggNode.planNode()).type() == AggType.STREAMING) {
      return expand(aggNode.child());
    }
    FlowNode aggFlow = new FlowNode(aggNode, FlowNodeType.AGG, "AGGREGATE");
    aggFlow.actualCard = aggNode.total(Attrib.AggFinalizeCounter.ROWS_RETURNED);
    aggFlow.expectedCard = aggNode.planNode().estCardinality();
    aggFlow.left = expand(aggNode.child());
    aggFlow.computeSelectivity();
    return aggFlow;
  }

  private FlowNode expandJoin(OperatorSynNode joinNode) {
    FlowNode joinFlow = new FlowNode(joinNode, FlowNodeType.JOIN, joinNode.title());
    joinFlow.actualCard = joinNode.total(Attrib.HashJoinCounter.ROWS_RETURNED);
    joinFlow.expectedCard = joinNode.planNode().estCardinality();
    joinFlow.left = expand(joinNode.leftChild());
    joinFlow.right = expand(joinNode.rightChild());
    joinFlow.computeSelectivity();
    if (joinFlow.left.expectedCard > joinFlow.right.expectedCard) {
      joinFlow.addNote("Wrong plan?");
    }
    if (joinFlow.left.actualCard > joinFlow.right.actualCard) {
      joinFlow.addNote("Misfire");
    }
    return joinFlow;
  }

  private FlowNode expandScan(OperatorSynNode scanNode) {
    FlowNode hdfsFlow = new FlowNode(scanNode, FlowNodeType.SCAN, "HDFS");
    hdfsFlow.actualCard = scanNode.total(Attrib.HdfsScanCounter.ROWS_RETURNED);
    hdfsFlow.expectedCard = scanNode.planNode().estCardinality();
    FlowNode source = new FlowNode(FlowNodeType.SOURCE, scanNode.fileFormat());
    source.actualCard = scanNode.estTotal(Attrib.HdfsScanCounter.ROWS_READ);
    source.expectedCard = ((HdfsScanNode) scanNode.planNode()).estRowCount();
    hdfsFlow.left = source;
    hdfsFlow.computeSelectivity();
    return hdfsFlow;
  }
}
