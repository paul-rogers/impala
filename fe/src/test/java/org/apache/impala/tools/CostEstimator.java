package org.apache.impala.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class CostEstimator {

  public enum NodeType {
    AGGREGATE("AGGREGATE", 1),
    EXCHANGE("EXCHANGE", 1),
    HASH_JOIN("HASH JOIN", 2),
    LOOP_JOIN("NESTED LOOP JOIN", 2),
    HDFS_SCAN("SCAN HDFS", 0),
    SOURCE("SOURCE", 0);

    private final String label_;
    private final int childCount_;

    private NodeType(String label, int childCount) {
      label_ = label;
      childCount_ = childCount;
    }

    public boolean isLeaf() { return childCount_ == 0; }
    public String label() { return label_; }

    public static NodeType fromLabel(String label) {
      for (NodeType type : values()) {
        if (type.label_.equals(label)) return type;
      }
      return null;
    }
  }

  public static class CostNode {

    // Analyzer uses 1 as cost of single operation.
    // We assume 100 instructions per op, and a 1 GHz CPU
    public static double CYCLES_PER_OP = 100;
    public static double CPU_CYCLES_PER_SEC = Math.pow(10, 9);
    // A cost of 1 in analyzer corresponds to SECS_PER_OP of time
    public static double COST_PER_SEC = CPU_CYCLES_PER_SEC / CYCLES_PER_OP;
    // Assume 100 MB/s of I/O rate
    public static double DISK_BYTES_PER_SEC = 100_000_000;
    public static double COST_PER_READ_BYTE = COST_PER_SEC / DISK_BYTES_PER_SEC;
    // Assume 1GB/s of network
    public static double NETWORK_BYTES_PER_SEC = Math.pow(10, 9);
    public static double COST_PER_NETWORK_BYTE = COST_PER_SEC / NETWORK_BYTES_PER_SEC;
    // Assumed processing cost per row - replace with conjunct cost, etc.
    public static double COST_PER_SCAN_ROW = 1000;
    // Assumed hash join build cost per row
    public static double COST_PER_HASH_JOIN_BUILD_ROW = 500;
    public static double COST_PER_HASH_JOIN_PROBE_ROW = 100;
    public static double COST_PER_AGG_ROW = 100;
    public static double COST_PER_LOOP_JOIN_ROW = 100;

    public static int BUILD_SIDE = 0;
    public static int PROBE_SIDE = 1;
    public static int OUTER_SIDE = 0;
    public static int INNER_SIDE = 1;

    NodeType type_;
    double nodeCost_;
    double netCost_;
    double bytes_;
    double rows_;
    List<CostNode> children_;

    public CostNode(NodeType type) {
      Preconditions.checkArgument(type != null);
      type_ = type;
    }

    public void setChild(CostNode child) {
      children_ = Lists.newArrayList(child);
    }

    public void addChild(CostNode child) {
      if (children_ == null) children_ = new ArrayList<>();
      children_.add(child);
    }

    public void computeCost() {
      if (children_ != null) {
        for (CostNode child : children_) {
          child.computeCost();
          netCost_ += child.netCost_;
        }
      }
      switch (type_) {
      case AGGREGATE:
        bytes_ = children_.get(0).bytes_;
        nodeCost_ = rows_ * COST_PER_AGG_ROW;
        break;
      case EXCHANGE:
        bytes_ = children_.get(0).bytes_;
        nodeCost_ = bytes_ * COST_PER_NETWORK_BYTE;
        break;
      case HASH_JOIN:
        // Assume all columns preserved: overly conservative
        bytes_ = (children_.get(0).rowWidth() + children_.get(1).rowWidth()) * rows_;
        nodeCost_ = children_.get(BUILD_SIDE).rows_ * COST_PER_HASH_JOIN_BUILD_ROW +
                children_.get(PROBE_SIDE).rows_ * COST_PER_HASH_JOIN_PROBE_ROW;
        break;
      case HDFS_SCAN:
        bytes_ = rows_ * children_.get(0).rowWidth();
        nodeCost_ = children_.get(0).nodeCost_;
        break;
      case LOOP_JOIN:
        bytes_ = (children_.get(0).rowWidth() + children_.get(1).rowWidth()) * rows_;
        nodeCost_ = children_.get(OUTER_SIDE).rows_ *
                children_.get(INNER_SIDE).rows_ * COST_PER_LOOP_JOIN_ROW;
        break;
      case SOURCE:
        nodeCost_ = bytes_ * COST_PER_READ_BYTE + rows_ * COST_PER_SCAN_ROW;
        break;
      default:
        break;
      }
      netCost_ += nodeCost_;
    }

    public double rowWidth() { return bytes_ / rows_; }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      treeToString(buf, "");
      return buf.toString();
    }

    private void treeToString(StringBuilder buf, String prefix) {
      buf.append(prefix)
         .append(type_.label()).append(": ")
         .append(formatLong("rows", rows_))
         .append(", ")
         .append(formatLong("bytes", bytes_))
         .append(", ")
         .append(formatCost("cost", nodeCost_))
         .append(", ")
         .append(formatCost("net cost", netCost_))
         .append("\n");
      if (children_ == null) return;
      for (CostNode child : children_) {
        child.treeToString(buf, prefix + "  ");
      }
    }

    private String formatLong(String label, double value) {
      return String.format("%s=%,d", label, (long) value);
    }

    private String formatCost(String label, double value) {
      return String.format("%s=%.3e", label, value);
    }
  }

  public CostNode buildModel(File plan) throws JsonProcessingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    JsonNode actualObj = mapper.readTree(plan);
    return parseNode(actualObj);
  }

  private CostNode parseNode(JsonNode jsonNode) {
    CostNode costNode = new CostNode(
        NodeType.fromLabel(jsonNode.get("name").asText()));
    costNode.rows_ = jsonNode.get("cardinality").asDouble();
    switch (costNode.type_) {
    case AGGREGATE:
    case EXCHANGE:
      costNode.setChild(parseNode(jsonNode.get("input")));
      break;
    case HASH_JOIN:
      costNode.addChild(parseNode(jsonNode.get("build")));
      costNode.addChild(parseNode(jsonNode.get("probe")));
      break;
    case HDFS_SCAN:
      parseHdfs(costNode, jsonNode);
      break;
    case LOOP_JOIN:
      costNode.addChild(parseNode(jsonNode.get("outer")));
      costNode.addChild(parseNode(jsonNode.get("inner")));
      break;
    default:
      break;

    }
    return costNode;
  }

  private void parseHdfs(CostNode costNode, JsonNode jsonNode) {
    CostNode source = new CostNode(NodeType.SOURCE);
    costNode.setChild(source);
    source.rows_ = jsonNode.get("input_cardinality").asDouble();
    source.bytes_ = jsonNode.get("read_bytes").asDouble();
  }

  @Test
  public void test() throws JsonProcessingException, IOException {
    File dir = new File("/home/progers/data/Prudential");
    File plan = new File(dir, "old_profile1-logical.json");
    CostNode node = buildModel(plan);
    System.out.println(node.toString());
    node.computeCost();
    System.out.println(node.toString());
  }
}
