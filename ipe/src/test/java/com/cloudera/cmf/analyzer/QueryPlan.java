package com.cloudera.cmf.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cmf.analyzer.ProfileAnalyzer.SummaryNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.SummaryNode.Attrib;
import com.google.common.base.Preconditions;

/**
 * Models the query plan as parsed from the text plan and text summary table in
 * the query profile. Describes plan-level properties of each operator, as well
 * as a few aggregate execution summary statistics.
 * <p>
 * The plan may be needed when scanning queries looking for those of interest.
 * Summary information is loaded only when requested to avoid wasted effort.
 */
public class QueryPlan {

  public static class PlanNode {
    protected final int operatorIndex;
    protected final String name;
    protected final String tail;
    protected boolean tailParsed;
    protected final List<String> details = new ArrayList<>();
    protected List<PlanNode> children;
    protected PlanNodeSummary summary;
    protected boolean detailsParsed;
    protected int hostCount;
    protected double perHostMemory;
    protected int tupleIds[];
    protected int estRowSize;
    protected int estCardinality;

    public PlanNode(int index, String name, String tail) {
      this.operatorIndex = index;
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

    public void parseTail() {
      if (tailParsed) { return; }
      doParseTail();
      tailParsed = true;
    }

    protected void doParseTail() { }

    public void print(PlanPrinter printer) {
      printer.write(heading());
      AttribBufFormatter fmt = new AttribBufFormatter(printer.detailsPrefix());
      format(fmt);
      printer.writePreFormatted(fmt.toString());
    }

    public String title() {
      String suffix = suffix();
      if (suffix == null) {
        suffix = "";
      } else {
        suffix = " [" + suffix + "]";
      }
      return String.format("%s %s",
          name, suffix);
    }

    public String heading() {
      return String.format("%d: %s",
          operatorIndex, title());
    }

    @Override
    public String toString() {
      AttribBufFormatter fmt = new AttribBufFormatter();
      fmt.line(heading());
      fmt.startGroup();
      format(fmt);
      fmt.endGroup();
      return fmt.toString();
    }

    public void format(AttribFormatter fmt) {
      if (tailParsed) {
        formatPlanTail(fmt);
      }
      if (detailsParsed) {
        formatPlanDetailAttribs(fmt);
      } else if (details != null) {
        formatPlanDetailText(fmt);
      }
      if (summary != null) {
        fmt.startGroup("Execution Summary");
        summary.format(fmt);
        fmt.endGroup();
      }
    }

    private void formatPlanDetailText(AttribFormatter fmt) {
      fmt.startGroup("Plan Details Text");
      for (String line : details) {
        fmt.line(line);
      }
      fmt.endGroup();
    }

    private void formatPlanDetailAttribs(AttribFormatter fmt) {
      if (detailsParsed) {
        fmt.startGroup("Plan Details");
        buildPlanDetails(fmt);
        fmt.endGroup();
      } else {
        fmt.startGroup("Plan Detail Text");
        for (String line : details) {
          fmt.line(line);
        }
        fmt.endGroup();
      }
    }

    protected void buildPlanDetails(AttribFormatter fmt) {
      fmt.attrib("Host Count", hostCount);
      fmt.attrib("Per Host Memory", perHostMemory);
      fmt.attrib("Tuple Ids", tupleIds);
      fmt.attrib("Est. Row Size", estRowSize);
      fmt.attrib("Est. Cardinality", estCardinality);
    }

    public void formatPlanTail(AttribFormatter fmt) {
      parseTail();
      buildPlanTail(fmt);
    }

    protected void buildPlanTail(AttribFormatter fmt) { }

    public String suffix() { return tail; }

    public void parsePlanDetails() {
      if (detailsParsed) { return; }
      parseDetailLines(0);
      detailsParsed = true;
    }

    protected int parseDetailLines(int posn) {
      parseHostLine(details.get(posn++));
      parseTupleLine(details.get(posn++));
      return posn;
    }

    // hosts=2 per-host-mem=unavailable
    // hosts=2 per-host-mem=10.00MB

    private void parseHostLine(String line) {
      Pattern p = Pattern.compile("hosts=(\\S+) per-host-mem=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      hostCount = Integer.parseInt(m.group(1));
      String mem = m.group(2);
      if (mem.equals("unavailable")) {
        perHostMemory = -1;
      } else {
        perHostMemory = ParseUtils.parsePlanMemory(mem);
      }
    }

    // tuple-ids=7 row-size=212B cardinality=1
    // tuple-ids=3,1,5,4,6 row-size=459B cardinality=1

   private void parseTupleLine(String line) {
      Pattern p = Pattern.compile("tuple-ids=(\\S+) row-size=(\\S+) cardinality=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      String parts[] = m.group(1).split(",");
      tupleIds = new int[parts.length];
      for (int i = 0; i < parts.length; i++) {
        tupleIds[i] = Integer.parseInt(parts[i]);
      }
      estRowSize = (int) Math.round(ParseUtils.parsePlanMemory(m.group(2)));
      estCardinality = Integer.parseInt(m.group(3));
    }

    public int hostCount() {
      parsePlanDetails();
      return hostCount;
    }

    public double perHostMemory() {
      parsePlanDetails();
      return perHostMemory;
    }

    public int[] tupleIds() {
      parsePlanDetails();
      return tupleIds;
    }

    public int estRowSize() {
      parsePlanDetails();
      return estRowSize;
    }

    public int estCardinality() {
      parsePlanDetails();
      return estCardinality;
    }

    public int operatorId() {
      return operatorIndex;
    }

    public boolean isLeaf() {
      return children == null || children.isEmpty();
    }
  }

  public static class AggregateNode extends PlanNode {
    public static final String NAME = "AGGREGATE";

    public enum AggType {
      STREAMING,
      FINALIZE
    }

    private AggType aggType;
    private String output[];
    private String groupBy[];

    public AggregateNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 1; }

    @Override
    protected void doParseTail() {
      aggType = AggType.valueOf(tail);
      Preconditions.checkState(aggType != null);
    }

    public AggType type() {
      if (aggType == null) {
        parseTail();
      }
      return aggType;
    }

    @Override
    protected int parseDetailLines(int posn) {
      parseOutputLine(details.get(posn++));
      parseGroupByLine(details.get(posn++));
      return super.parseDetailLines(posn);
    }

    // output: max(a16.wshe_name), max(concat(ifnull(a13.prvsn_commdty_nm, ' '), ...

    private void parseOutputLine(String line) {
      output = ParseUtils.parseExprs(line.substring("output: ".length()));
    }

    // group by: CAST(a11.selling_org_id AS INT), ...

    private void parseGroupByLine(String line) {
      groupBy = ParseUtils.parseExprs(line.substring("group by: ".length()));
    }

    @Override
    protected void buildPlanDetails(AttribFormatter fmt) {
      fmt.list("Output", output);
      fmt.list("Group By", groupBy);
      super.buildPlanDetails(fmt);
    }
  }

  public static class ExchangeNode extends PlanNode {
    public static final String NAME = "EXCHANGE";

    public enum ExchangeType {
      BROADCAST,
      HASH,
      UNPARTITIONED
    }

    private String[] project;
    private ExchangeType exchangeType;

    public ExchangeNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 1; }

    @Override
    protected void doParseTail() {
      Pattern p = Pattern.compile("([^(]+)\\((.*)\\)");
      Matcher m = p.matcher(tail);
      if (m.matches()) {
        exchangeType = ExchangeType.valueOf(m.group(1));
        project = ParseUtils.parseExprs(m.group(2));
      } else {
        exchangeType = ExchangeType.valueOf(tail);
        project = new String[0];
      }
    }

    @Override
    public String suffix() {
      return exchangeType == null
          ? super.suffix()
          : exchangeType.name();
    }

    @Override
    protected void buildPlanTail(AttribFormatter fmt) {
      if (exchangeType == ExchangeType.HASH) {
        fmt.list("Keys", project);
      }
      super.buildPlanTail(fmt);
    }

    public ExchangeType type() {
      parseTail();
      return exchangeType;
    }

    public String[] project() {
      parseTail();
      return project;
    }
  }

  public static class ScanRef {
    private final String itemName;
    private final String alias;

    public ScanRef(String itemName, String alias) {
      this.itemName = itemName;
      this.alias = alias;
    }

    public String itemName() { return itemName; }
    public String alias() { return alias; }

    @Override
    public String toString() {
      if (alias == null) {
        return itemName;
      } else {
        return itemName + " " + alias;
      }
    }
  }

  public static class HdfsScanNode extends PlanNode {
    public static final String NAME = "SCAN HDFS";

    private List<ScanRef> project;
    private String scanType;
    private int partitionIndex;
    private int partitionCount;
    private int fileCount;
    private int partitionSize;
    private int estRowCount;
    private String columnStats;
    private String[] predicates;
    private String[] filters;

    public HdfsScanNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 0; }

    @Override
    protected void doParseTail() {
      project = new ArrayList<>();
      String parts[] = tail.split(", ");
      for (int i = 0; i < parts.length - 1; i++) {
        String projectParts[] = parts[i].split(" ");
        project.add(new ScanRef(projectParts[0],
            projectParts.length == 0 ? null : projectParts[1]));
      }
      scanType = parts[parts.length - 1];
    }

    @Override
    public String suffix() {
      return scanType == null
          ? super.suffix()
          : scanType;
    }

    @Override
    protected void buildPlanTail(AttribFormatter fmt) {
      fmt.list("Project", project);
      super.buildPlanTail(fmt);
    }

    public String type() {
      parseTail();
      return scanType;
    }

    @Override
    protected int parseDetailLines(int posn) {
      parsePartitionsLine(details.get(posn++));
      String line = details.get(posn);
      if (line.startsWith("predicates:")) {
        parsePredicatesLine(line);
        line = details.get(++posn);
      }
      if (line.startsWith("runtime filters:")) {
        parseFiltersLine(line);
        line = details.get(++posn);
      }
      parseTableStatsLine(line);
      posn++;
      parseColumnStatsLine(details.get(posn++));
      return super.parseDetailLines(posn);
    }

    // partitions=1/1 files=1 size=6.65KB

    private void parsePartitionsLine(String line) {
      Pattern p = Pattern.compile("partitions=(\\d+)/(\\d+) files=(\\d+) size=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      partitionIndex = Integer.parseInt(m.group(1));
      partitionCount = Integer.parseInt(m.group(2));
      fileCount = Integer.parseInt(m.group(3));
      partitionSize = (int) Math.round(ParseUtils.parsePlanMemory(m.group(4)));
    }

    // predicates: a15.invc_prvsn_commdty_id = 320144, ...

    private void parsePredicatesLine(String line) {
      predicates = ParseUtils.parseExprs(line.substring("predicates: ".length()));
    }

    // runtime filters: RF001 -> CAST(a15.selling_org_id AS INT), ...

    private void parseFiltersLine(String line) {
      filters = ParseUtils.parseExprs(line.substring("runtime filters: ".length()));
    }

    // table stats: 154 rows total

    private void parseTableStatsLine(String line) {
      Pattern p = Pattern.compile("table stats: (\\d+) rows total");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      estRowCount = Integer.parseInt(m.group(1));
    }

    // column stats: all

    private void parseColumnStatsLine(String line) {
      Pattern p = Pattern.compile("column stats: (.*)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      // TODO: Extract more info?
      columnStats = m.group(1);
    }

    public int partitionIndex() {
      parsePlanDetails();
      return partitionIndex;
    }

    public int partitionCount() {
      parsePlanDetails();
      return partitionCount;
    }

    public int fileCount() {
      parsePlanDetails();
      return fileCount;
    }

    public int partitionSize() {
      parsePlanDetails();
      return partitionSize;
    }

    public int estRowCount() {
      parsePlanDetails();
      return estRowCount;
    }

    public String columnStats() {
      parsePlanDetails();
      return columnStats;
    }

    @Override
    protected void buildPlanDetails(AttribFormatter fmt) {
      fmt.attrib("Partition", partitionIndex);
      fmt.attrib("Partition Count", partitionCount);
      fmt.attrib("File Count", fileCount);
      fmt.attrib("Partition Size", partitionSize);
      fmt.list("Predicates", predicates);
      fmt.list("Filter", filters);
      fmt.attrib("Est. Row Count", estRowCount);
      fmt.attrib("Column Stats", columnStats);
      super.buildPlanDetails(fmt);
    }
  }

  public static class JoinNode extends PlanNode {
    public static final String HASH_JOIN_NAME = "HASH JOIN";

    public enum JoinType {
      INNER,
      OUTER
    }

    public enum DistribType {
      PARTITIONED,
      BROADCAST
    }

    private JoinType joinType;
    private DistribType distribType;
    private String[] hashPredicates;
    private String[] filters;

    public JoinNode(int index, String name, String tail) {
      super(index, name, tail);
    }

    @Override
    public int inputCount() { return 2; }

    @Override
    protected void doParseTail() {
      if (joinType != null) { return; }
      String parts[] = tail.split(", ");
      for (String part : parts) {
        switch (part) {
        case "BROADCAST":
          distribType = DistribType.BROADCAST;
          break;
        case "PARTITIONED":
          distribType = DistribType.PARTITIONED;
          break;
        case "INNER JOIN":
          joinType = JoinType.INNER;
          break;
        case "OUTER_JOIN":
          joinType = JoinType.OUTER;
          break;
        default:
          throw new IllegalStateException("Join type");
        }
      }
      Preconditions.checkNotNull(joinType);
      Preconditions.checkNotNull(distribType);
    }

    public JoinType joinType() {
      if (joinType == null) {
        parseTail();
      }
      return joinType;
    }

    public DistribType distribType() {
      if (distribType == null) {
        parseTail();
      }
      return distribType;
    }

    @Override
    protected int parseDetailLines(int posn) {
      String line = details.get(posn);
      if (line.startsWith("hash predicates:")) {
        parsePredicateLine(line);
        line = details.get(++posn);
      }
      if (line.startsWith("runtime filters: ")) {
        parseFiltersLine(line);
        posn++;
      }
      return super.parseDetailLines(posn);
    }

    // hash predicates: CAST(a11.selling_org_id AS INT) = a16.wshe_number, ...

    private void parsePredicateLine(String line) {
      hashPredicates = ParseUtils.parseExprs(line.substring("hash predicates: ".length()));
    }

    // runtime filters: RF001 <- a16.wshe_number, ...

    private void parseFiltersLine(String line) {
      filters = ParseUtils.parseExprs(line.substring("runtime filters: ".length()));
    }

    @Override
    protected void buildPlanDetails(AttribFormatter fmt) {
      fmt.list("Hash Predicates", hashPredicates);
      fmt.list("Filters", filters);
      super.buildPlanDetails(fmt);
    }
  }

  private final SummaryNode query;
  private final String textPlan;
  private PlanNode[] nodes;
  private List<String> details = new ArrayList<>();
  private QueryPlan.PlanNode root;
  private long perHostMemory;
  private int perHostVCores;
  private boolean hasSummary;
  private boolean parsedTail;
  private boolean planDetailsParsed;

  public QueryPlan(SummaryNode query) {
    this.query = query;
    textPlan = query.attrib(SummaryNode.Attrib.PLAN);
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

  public void parseTail() {
    if (parsedTail) { return; }
    for (int i = 0; i < nodes.length; i++) {
      nodes[i].parseTail();
    }
    parsedTail = true;
  }

  public void parseSummary() {
    if (hasSummary) { return; }
    try {
      BufferedReader in = new BufferedReader(
          new StringReader(query.attrib(Attrib.EXEC_SUMMARY)));
      String line = in.readLine();
      line = in.readLine();
      line = in.readLine();
      while((line = in.readLine()) != null) {
        PlanNodeSummary summary = new PlanNodeSummary();
        int index = summary.parseSummary(line);
        nodes[index].setSummary(summary);
      }
      hasSummary = true;
    } catch (IOException e) {
      // Should never occur
      throw new IllegalStateException();
    }
  }

  public QueryPlan.PlanNode root() { return root; }

  public void parsePlanDetails() {
    if (planDetailsParsed) { return; }
    for (int i = 0; i < nodes.length; i++) {
      nodes[i].parsePlanDetails();
    }
    planDetailsParsed = true;
  }

  public int operatorCount() {
    return nodes.length;
  }

  public PlanNode operator(int i) {
    return nodes[i];
  }
}
