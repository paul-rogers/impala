package com.cloudera.cmf.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Period;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.cloudera.cmf.analyzer.ProfileAnalyzer.QueryNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.QueryNode.Attrib;
import com.cloudera.cmf.analyzer.QueryPlan.AggregateNode;
import com.cloudera.cmf.analyzer.QueryPlan.ExchangeNode;
import com.cloudera.cmf.analyzer.QueryPlan.HdfsScanNode;
import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;

public class QueryPlan {

  public static class PlanNodeSummary {
    // Operator          #Hosts   Avg Time   Max Time  #Rows  Est. #Rows   Peak Mem  Est. Peak Mem  Detail
    // |--19:EXCHANGE         2   12.135us   12.324us    154         154          0              0  HASH(a16.wshe_number,a16.ws...

    private static String TIME_PATTERN = "(\\S+)\\s+";
    private static String ROW_PATTERN = "([0-9.]+)([A-Z]*)\\s+";
    private static String MEM_PATTERN = "([-0-9.]+) ([A-Z]*)\\s+";
    private static String SUMMARY_PATTERN =
        "[ |-]*(\\d+):[A-Z ]+\\s+" + // Operator
        "(\\d)+\\s+" + // #Hosts
        TIME_PATTERN + // Avg Time
        TIME_PATTERN + // Max Time
        ROW_PATTERN + // #Rows
        ROW_PATTERN + // Est. #Rows
        MEM_PATTERN + // Peak Mem
        MEM_PATTERN + // Est. Peak Mem
        ".*"; // Detail

    public static long US_PER_MS = 1000;
    public static long US_PER_SEC = US_PER_MS * 1000;
    public static long US_PER_MIN = US_PER_SEC * 60;
    public static long US_PER_HOUR = US_PER_MIN * 60;

    public int hostCount;
    public double aveTimeUs;
    public double maxTimeUs;
    public double estRowCount;
    public double actualRowCount;
    public double estMem;
    public double actualMem;

    public int parseSummary(String line) {
      Pattern p = Pattern.compile(SUMMARY_PATTERN);
      Matcher m = p.matcher(line);
      if (! m.matches()) {
        throw new IllegalStateException("Summary line");
      }
      int index = Integer.parseInt(m.group(1));
      hostCount = Integer.parseInt(m.group(2));
      aveTimeUs = parseTime(m.group(3));
      maxTimeUs = parseTime(m.group(4));
      actualRowCount = parseRows(m.group(5), m.group(6));
      estRowCount = parseRows(m.group(7), m.group(8));
      actualMem = parseMem(m.group(9), m.group(10));
      estMem = parseMem(m.group(11), m.group(12));
      return index;
    }

    private static double parseTime(String valueStr) {
      Pattern p = Pattern.compile("([0-9.]+)us");
      Matcher m = p.matcher(valueStr);
      if (m.matches()) {
        return Double.parseDouble(m.group(1));
      }
      p = Pattern.compile("((\\d+)h)?((\\d+)m)?((\\d+)s)?(([0-9.]+)ms)?");
      m = p.matcher(valueStr);
      if (! m.matches()) {
        throw new IllegalStateException("Duration format: " + valueStr);
      }
      double value = 0;
      if (m.group(1) != null) {
        value = Integer.parseInt(m.group(2)) * US_PER_HOUR;
      }
      if (m.group(3) != null) {
        value += Integer.parseInt(m.group(4)) * US_PER_MIN;
      }
      if (m.group(5) != null) {
        value += Integer.parseInt(m.group(6)) * US_PER_SEC;
      }
      if (m.group(7) != null) {
        value += Double.parseDouble(m.group(8)) * US_PER_MS;
      }
      return value;
    }

    private static double parseRows(String valueStr, String unitsStr) {
      double value = Double.parseDouble(valueStr);
      switch (unitsStr) {
      case "K":
        value *= 1000; // TODO: K = 1000 or 1024?
        break;
      case "M":
        value *= 1000 * 1000; // TODO: Same
        break;
      case "":
        break;
      default:
        throw new IllegalStateException("Row Suffix: " + unitsStr);
      }
      return value;
    }

    private static double parseMem(String valueStr, String unitsStr) {
      double value = Double.parseDouble(valueStr);
      value = Math.max(0, value); // Some rows report negative estimates.
      switch (unitsStr) {
      case "KB":
        value *= 1024; // TODO: K = 1000 or 1024?
        break;
      case "MB":
        value *= 1024 * 1024; // TODO: Same
        break;
      case "":
      case "B":
        break;
      default:
        throw new IllegalStateException("Mem Suffix: " + unitsStr);
      }
      return value;
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("Hosts: ")
        .append(hostCount)
        .append("\nAvg Time (s): ")
        .append(toSecs(aveTimeUs))
        .append("\nMax Time (s): ")
        .append(toSecs(maxTimeUs))
        .append("\nRows: ")
        .append(format(actualRowCount))
        .append("\nRows (est.): ")
        .append(format(estRowCount))
        .append("\nPeak Memory: ")
        .append(format(actualMem))
        .append("\nPeak Memory (est.): ")
        .append(format(estMem))
        .append("\n")
        .toString();
    }

    public static String toSecs(double us) {
      return String.format("%,.3f", us / 1000 / 1000);
    }

    public static String format(double value) {
      return String.format("%,.0f", value);
    }
  }

  public static class PlanNode {
    private final int index;
    private final String name;
    private final String tail;
    private final List<String> details = new ArrayList<>();
    private List<PlanNode> children;
    private PlanNodeSummary summary;

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

    public void setSummary(PlanNodeSummary summary) {
      this.summary = summary;
    }

    public void print(PlanPrinter printer) {
      String suffix;
      if (tail == null) {
        suffix = "";
      } else {
        suffix = " [" + tail + "]";
      }
      printer.write(String.format("%d: %s %s",
          index, name, suffix));
      if (summary != null) {
        printer.writeBlock(summary.toString());
      }
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
  private boolean hasSummary;

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

  public void parseSummary() {
    if (hasSummary) { return; }
    try {
      BufferedReader in = new BufferedReader(
          new StringReader(query.attrib(Attrib.EXECSUMMARY)));
      String line = in.readLine();
      line = in.readLine();
      line = in.readLine();
      while((line = in.readLine()) != null) {
        PlanNodeSummary summary = new PlanNodeSummary();
        int index = summary.parseSummary(line);
        nodes[index].setSummary(summary);
      }

    } catch (IOException e) {
      // Should never occur
      throw new IllegalStateException();
    }

  }

  public QueryPlan.PlanNode root() { return root; }
}