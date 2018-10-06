package com.cloudera.cmf.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cmf.analyzer.ProfileAnalyzer.QueryNode;
import com.cloudera.cmf.analyzer.QueryPlan.AggregateNode;
import com.cloudera.cmf.analyzer.QueryPlan.ExchangeNode;
import com.cloudera.cmf.analyzer.QueryPlan.HdfsScanNode;
import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;

public class QueryPlan {

  public static class PlanNode {
    private final int index;
    private final String name;
    private final String tail;
    private final List<String> details = new ArrayList<>();
    private List<PlanNode> children;

    public PlanNode(int index, String name, String tail) {
      this.index = index;
      this.name = name;
      this.tail = tail;
    }

    public void addDetail(String line) {
      details.add(line);
    }

    public int inputCount() { return 1; }

    public void addChild(PlanNode node) {
      if (children == null) {
        children = new ArrayList<>();
      }
      children.add(0, node);
    }

    public List<PlanNode> children() { return children; }

    public void print(PlanPrinter printer) {
      String suffix;
      if (tail == null) {
        suffix = "";
      } else {
        suffix = " [" + tail + "]";
      }
      printer.write(String.format("%d: %s %s",
          index, name, suffix));
    }
  }

  public static class AggregateNode extends PlanNode {
    public static final String NAME = "AGGREGATE";

    public AggregateNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 1; }
  }

  public static class ExchangeNode extends PlanNode {
    public static final String NAME = "EXCHANGE";

    public ExchangeNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 1; }
  }

  public static class HdfsScanNode extends PlanNode {
    public static final String NAME = "SCAN HDFS";

    public HdfsScanNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 0; }
  }

  public static class JoinNode extends PlanNode {
    public static final String HASH_JOIN_NAME = "HASH JOIN";

    public JoinNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 2; }
  }

  private final QueryNode query;
  private final String textPlan;
  private PlanNode[] nodes;
  private List<String> details = new ArrayList<>();
  private QueryPlan.PlanNode root;
  private long perHostMemory;
  private int perHostVCores;

  public QueryPlan(QueryNode query) {
    this.query = query;
    textPlan = query.attrib(QueryNode.Attrib.PLAN);
    try {
      buildPlan();
    } catch (IOException e) {
      // Should never occur
      throw new IllegalStateException(e);
    }
  }

  private void buildPlan() throws IOException {
    BufferedReader in = new BufferedReader(
        new StringReader(textPlan));
    String line = in.readLine();
    while((line = in.readLine()) != null) {
      if (line.isEmpty()) { break; }
      if (line.startsWith("---")) { continue; }
      details.add(line);
    }
    parseNode(in, null, 0);
  }

  private void parseNode(BufferedReader in, PlanNode parent, int depth) throws NumberFormatException, IOException {
    String line = in.readLine();
    if (line.startsWith("--")) {
      throw new IllegalStateException("Parser error");
    }
    if (depth > 0) {
      line = line.substring(depth * 3);
    }
    Pattern p = Pattern.compile( "(\\d+):([^\\[]+)(?: \\[([^\\]]+)\\])?");
    Matcher m = p.matcher(line);
    if (! m.matches()) {
      throw new IllegalStateException("Unknown header");
    }
    int index = Integer.parseInt(m.group(1));
    String name = m.group(2);
    String tail = m.group(3);
    QueryPlan.PlanNode node;
    switch(name) {
    case QueryPlan.AggregateNode.NAME:
      node = new QueryPlan.AggregateNode(index, name, tail);
      break;
    case QueryPlan.ExchangeNode.NAME:
      node = new QueryPlan.ExchangeNode(index, name, tail);
      break;
    case QueryPlan.HdfsScanNode.NAME:
      node = new QueryPlan.HdfsScanNode(index, name, tail);
      break;
    case JoinNode.HASH_JOIN_NAME:
      node = new JoinNode(index, name, tail);
      break;
    default:
      throw new IllegalStateException("Unknown node: " + name);
    }
    if (parent == null) {
      nodes = new PlanNode[index+1];
      root = node;
    } else {
      parent.addChild(node);
    }
    nodes[index] = node;

    while ((line = in.readLine()) != null) {
      if (line.startsWith( "--")) { break; }
      int prefix = (depth + 1) * 3;
      if (line.length() <= prefix) { break; }
      line = line.substring(prefix);
      node.addDetail(line);
    }

    for (int i = node.inputCount() - 1; i >= 0; i--) {
      parseNode(in, node, depth + i);
    }
  }

  public QueryPlan.PlanNode root() { return root; }
}