package com.cloudera.cmf.profile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cmf.printer.AttribBufFormatter;
import com.cloudera.cmf.printer.AttribFormatter;
import com.cloudera.cmf.printer.PlanPrinter;
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

  /**
   * Represents a fragment as described in a query plan from
   * 2.9.0 onwards. (Prior versions of the plan did not show
   * fragments; they had to be gleaned from the execution detail.)
   */
  public static class PlanFragment {
    protected final int fragmentId;
    protected final String details;
    protected final int hostCount;
    protected final int instanceCount;

    public PlanFragment(int fragmentId, String details, int hostCount, int instanceCount) {
      this.fragmentId = fragmentId;
      this.details = details;
      this.hostCount = hostCount;
      this.instanceCount = instanceCount;
    }
  }

  /**
   * Represents an operator within the plan. Does not represent the
   * "PLAN ROOT" introduced in 2.9.0 since that is not an operator
   * and has no operator-id. A node is associated with the parser
   * used to parse the plan, and that is used to parse the "details"
   * for the plan on demand.
   */
  public static abstract class PlanNode {
    public enum PlanNodeType {
      ROOT, EXCHANGE, AGG, JOIN, HDFS_SCAN
    }

    public static final int ROOT_OPERATOR_ID = -1;

    protected final PlanParser parser;
    protected int operatorId;
    protected final String name;
    protected final String tail;
    protected boolean detailsParsed;
    protected PlanFragment fragment;
    protected List<String> details;
    protected List<PlanNode> children;
    protected PlanNodeSummary summary;
    protected boolean tailParsed;
    // Before 2.9.0. After, check the fragment.
    protected int hostCount;
    // Before 2.9.0 per-host-memory, after mem-estimate
    protected double perHostMemory;
    // 2.9.0 and later
    protected double reservationMemory;
    protected int tupleIds[];
    protected int estRowSize;
    protected int estCardinality;
    public double memReservation;

    public PlanNode(PlanParser parser, int index, String name, String tail) {
      this.parser = parser;
      this.operatorId = index;
      this.name = name;
      this.tail = tail;
    }

    public abstract PlanNodeType type();

    public void addDetail(String line) {
      details.add(line);
    }

    public int inputCount() { return 1; }
    public boolean isRoot() { return type() == PlanNodeType.ROOT; }

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

    /**
     * Parse the node tail: the part after the operator name:
     */
    public void parseTail() {
      if (tailParsed) { return; }
      parser.parseTail(this);
      tailParsed = true;
    }

    public void parseDetails() {
      if (detailsParsed) { return; }
      parser.parseDetails(this);
      detailsParsed = true;
    }

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
          operatorId, title());
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
      parseTail();
      parseDetails();
      formatAttribs(fmt);
      if (summary != null) {
        fmt.startGroup("Execution Summary");
        summary.format(fmt);
        fmt.endGroup();
      }
    }

    protected void formatAttribs(AttribFormatter fmt) {
      fmt.attrib("Host Count", hostCount());
      fmt.attrib("Instance Count", instanceCount());
      if (fragment != null) {
        fmt.attrib("Fragment ID", fragment.fragmentId);
      }
      fmt.attrib("Per Host Memory", perHostMemory);
      fmt.attrib("Memory Reservation", memReservation);
      if (! isRoot()) {
        fmt.attrib("Tuple Ids", tupleIds);
        fmt.attrib("Est. Row Size", estRowSize);
        fmt.attrib("Est. Cardinality", estCardinality);
      }
    }

    public String suffix() { return tail; }

    public void parsePlanDetails() {
      if (detailsParsed) { return; }
      parser.parseDetails(this);
      detailsParsed = true;
    }

    public int hostCount() {
      // 2.9.0 and later: host count is in fragment
      if (fragment != null) { return fragment.hostCount; }
      // Prior: host count is in the details
      parsePlanDetails();
      return hostCount;
    }

    public int instanceCount() {
      // 2.9.0 and later: instance count is in fragment
      // Prior: no instance count, assume same as host count
      return (fragment == null) ? hostCount() : fragment.instanceCount;
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
      return operatorId;
    }

    public boolean isLeaf() {
      return children == null || children.isEmpty();
    }

    public PlanFragment fragment() {
      return fragment;
    }
  }

  public static class PlanRootNode extends PlanNode {

    public static final String NAME = "PLAN-ROOT SINK";

    public PlanRootNode(PlanParser parser) {
      super(parser, -1, NAME, "");

      // Nothing to parse
      tailParsed = true;
      detailsParsed = true;
    }

    @Override
    public PlanNodeType type() { return PlanNodeType.ROOT; }
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

    public AggregateNode(PlanParser parser, int index, String name, String tail) {
      super(parser, index, name, tail);
    }

    @Override
    public PlanNodeType type() { return PlanNodeType.AGG; }

    @Override
    public int inputCount() { return 1; }

    public AggType aggType() {
      if (aggType == null) {
        parseTail();
      }
      return aggType;
    }

    @Override
    protected void formatAttribs(AttribFormatter fmt) {
      fmt.attrib("Agg Type", aggType.name());
      fmt.list("Output", output);
      fmt.list("Group By", groupBy);
      super.formatAttribs(fmt);
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

    public ExchangeNode(PlanParser parser, int index, String name, String tail) {
      super(parser, index, name, tail);
    }

    @Override
    public PlanNodeType type() { return PlanNodeType.EXCHANGE; }

    @Override
    public int inputCount() { return 1; }

    @Override
    public String suffix() {
      return exchangeType == null
          ? super.suffix()
          : exchangeType.name();
    }

    public ExchangeType exchangeType() {
      parseTail();
      return exchangeType;
    }

    @Override
    protected void formatAttribs(AttribFormatter fmt) {
      fmt.attrib("Exchange Type", exchangeType().name());
      if (exchangeType == ExchangeType.HASH) {
        fmt.list("Keys", project);
      }
      super.formatAttribs(fmt);
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

    private String scanType;
    private String tableName;
    private int partitionIndex;
    private int partitionCount;
    private int fileCount;
    private int partitionSize;
    private int estRowCount;
    private String columnStats;
    private String[] predicates;
    private String[] filters;
    private String[] parquetDictPredicates;

    public HdfsScanNode(PlanParser parser, int index, String name, String tail) {
      super(parser, index, name, tail);
    }

    @Override
    public PlanNodeType type() { return PlanNodeType.HDFS_SCAN; }

    @Override
    public int inputCount() { return 0; }

    @Override
    public String suffix() {
      if (tailParsed) {
        return tableName + ", " + scanType;
      } else {
        return tail;
      }
    }

    public String scanType() {
      parseTail();
      return scanType;
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
    protected void formatAttribs(AttribFormatter fmt) {
      fmt.attrib("Scan Type", scanType);
      fmt.attrib("Table", tableName);
      fmt.attrib("Partition", partitionIndex);
      fmt.attrib("Partition Count", partitionCount);
      fmt.attrib("File Count", fileCount);
      fmt.attrib("Partition Size", partitionSize);
      fmt.list("Predicates", predicates);
      fmt.list("Filter", filters);
      fmt.list("Parquet Dict. Predicates", parquetDictPredicates);
      fmt.attrib("Est. Row Count", estRowCount);
      fmt.attrib("Column Stats", columnStats);
      super.formatAttribs(fmt);
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

    public JoinNode(PlanParser parser, int index, String name, String tail) {
      super(parser, index, name, tail);
    }

    @Override
    public PlanNodeType type() { return PlanNodeType.JOIN; }

    @Override
    public int inputCount() { return 2; }

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
    protected void formatAttribs(AttribFormatter fmt) {
      fmt.list("Hash Predicates", hashPredicates);
      fmt.list("Filters", filters);
      super.formatAttribs(fmt);
    }
  }

  /**
   * The query plan is available only in text form. The details of the
   * text format change from one version to another. This class
   * encapsulates the version-specific parsing logic. In general,
   * each new version extends the prior version and changes the bit
   * of the parser as needed to handle specific changes in plan format.
   * Ugly, but is the simplest solution until we serialized the plan
   * in something like JSON.
   */
  public abstract static class PlanParser {

    QueryPlan plan;
    public String version;
    BufferedReader in;

    public PlanParser(QueryPlan plan, String version) {
      this.plan = plan;
      this.version = version;
    }

    public static PlanParser createParser(QueryPlan plan) {

      String version = plan.profile.version();
      if (version.compareTo("2.9.0") < 0) {
        return new Ver2_6_0Parser(plan, version);
      } else {
        return new Ver2_9_0Parser(plan, version);
      }
    }

    /**
     * Parse the plan into nodes (and, where available, fragments.)
     * Captures the plan details as text lines, to be parsed later
     * only when requested.
     */
    public void parse() throws IOException {
      in = new BufferedReader(
          new StringReader(plan.textPlan));
      String line = in.readLine();
      while((line = in.readLine()) != null) {
        if (line.isEmpty()) { break; }
        if (line.startsWith("---")) { continue; }
        plan.details.add(line);
      }
      parsePlan();
      in.close();
      in = null;
    }

    protected abstract void parsePlan();

    protected abstract void parseTail(PlanNode node);

    protected abstract void parseDetails(PlanNode node);

    protected String nextLine() {
      try {
        return in.readLine();
      } catch (IOException e) {
        throw new IllegalStateException("Plan parser", e);
      }
    }

    protected List<String> parseDetailLines(int depth) {
      List<String> lines = new ArrayList<>();
      String line;
      while ((line = nextLine()) != null) {
        if (line.startsWith( "--")) { break; }
        int prefix = (depth + 1) * 3;
        if (line.length() <= prefix) { break; }
        line = line.substring(prefix);
        lines.add(line);
      }
      return lines;
    }
  }

  public static class Ver2_6_0Parser extends PlanParser {

    public Ver2_6_0Parser(QueryPlan plan, String version) {
      super(plan, version);
    }

    @Override
    protected void parsePlan() {
      // 2.9.0 and later has an un-numbered root node. Insert a
      // fake node in prior versions.
      plan.root = new PlanRootNode(this);
      parseNode(null, 0);
    }

    protected void parseNode(PlanNode parent, int depth) {
      String line = nextLine();
      doParseNode(line, parent, depth);
    }

    protected void doParseNode(String line, PlanNode parent, int depth)
    {
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
        node = new QueryPlan.AggregateNode(this, index, name, tail);
        break;
      case QueryPlan.ExchangeNode.NAME:
        node = new QueryPlan.ExchangeNode(this, index, name, tail);
        break;
      case QueryPlan.HdfsScanNode.NAME:
        node = new QueryPlan.HdfsScanNode(this, index, name, tail);
        break;
      case JoinNode.HASH_JOIN_NAME:
        node = new JoinNode(this, index, name, tail);
        break;
      default:
        throw new IllegalStateException("Unknown node: " + name);
      }
      if (parent == null) {
        // Plan in 2.9.0 and later has an unnumbered root node.
        // To keep things simple, give this node a fake operator ID
        // that is one greater than the largest used in the plan.
        // (Fortunately, the largest-numbered operator appears at the
        // top of the text plan.)
        plan.nodes = new PlanNode[index+2];
        plan.root.operatorId = index + 1;
        plan.nodes[index + 1] = plan.root;
        plan.root.addChild(node);
        extendNode(plan.root);
      } else {
        parent.addChild(node);
      }
      plan.nodes[index] = node;
      node.details = parseDetailLines(depth);
      extendNode(node);

      for (int i = node.inputCount() - 1; i >= 0; i--) {
        parseNode(node, depth + i);
      }
    }

    protected void extendNode(PlanNode node) { }

    @Override
    public void parseTail(PlanNode node) {
      switch (node.type()) {
      case AGG:
        parseAggTail((AggregateNode) node);
        break;
      case EXCHANGE:
        parseExchangeTail((ExchangeNode) node);
        break;
      case HDFS_SCAN:
        parseHdfsScanTail((HdfsScanNode) node);
        break;
      case JOIN:
        parseJoinTail((JoinNode) node);
        break;
      case ROOT:
        break;
      default:
        throw new IllegalStateException("Node type");
      }
    }

    private void parseJoinTail(JoinNode node) {
      String parts[] = node.tail.split(", ");
      for (String part : parts) {
        switch (part) {
        case "BROADCAST":
          node.distribType = JoinNode.DistribType.BROADCAST;
          break;
        case "PARTITIONED":
          node.distribType = JoinNode.DistribType.PARTITIONED;
          break;
        case "INNER JOIN":
          node.joinType = JoinNode.JoinType.INNER;
          break;
        case "OUTER_JOIN":
          node.joinType = JoinNode.JoinType.OUTER;
          break;
        default:
          throw new IllegalStateException("Join type");
        }
      }
      Preconditions.checkNotNull(node.joinType);
      Preconditions.checkNotNull(node.distribType);
    }

    private void parseAggTail(AggregateNode node) {
      node.aggType = AggregateNode.AggType.valueOf(node.tail);
      Preconditions.checkState(node.aggType != null);
    }

    // 01:EXCHANGE [UNPARTITIONED]
    private void parseExchangeTail(ExchangeNode node) {
      Pattern p = Pattern.compile("([^(]+)\\((.*)\\)");
      Matcher m = p.matcher(node.tail);
      if (m.matches()) {
        node.exchangeType = ExchangeNode.ExchangeType.valueOf(m.group(1));
        node.project = ParseUtils.parseExprs(m.group(2));
      } else {
        node.exchangeType = ExchangeNode.ExchangeType.valueOf(node.tail);
        node.project = new String[0];
      }
    }

    // 00:SCAN HDFS [uacc_df2_abw.cmn_ops_parm, RANDOM]
    private void parseHdfsScanTail(HdfsScanNode node) {
      String parts[] = node.tail.split(", ");
      node.tableName = parts[0];
      node.scanType = parts[1];
    }

    @Override
    public void parseDetails(PlanNode node) {
      int posn = parseOperDetails(node);
      parseHostLine(node, posn++);
      parseTupleLine(node, posn++);
    }

    // hosts=2 per-host-mem=unavailable
    // hosts=2 per-host-mem=10.00MB

    protected int parseOperDetails(PlanNode node) {
      int posn;
      switch (node.type()) {
      case ROOT:
        posn = parseRootDetails((PlanRootNode) node);
        break;
      case AGG:
        posn = parseAggDetails((AggregateNode) node);
        break;
      case EXCHANGE:
        posn = parseExchangeDetails((ExchangeNode) node);
        break;
      case JOIN:
        posn = parseJoinDetails((JoinNode) node);
        break;
      case HDFS_SCAN:
        posn = parseHdfsDetails((HdfsScanNode) node);
        break;
      default:
        throw new IllegalStateException("Node type");
      }
      return posn;
    }

    protected int parseRootDetails(PlanRootNode node) {
      return 0;
    }

    private int parseAggDetails(AggregateNode node) {
      parseAggOutputLine((AggregateNode) node, 0);
      parseAggGroupByLine((AggregateNode) node, 1);
      return 2;
    }

    private void parseHostLine(PlanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("hosts=(\\S+) per-host-mem=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      node.hostCount = Integer.parseInt(m.group(1));
      String mem = m.group(2);
      if (mem.equals("unavailable")) {
        node.perHostMemory = -1;
      } else {
        node.perHostMemory = ParseUtils.parsePlanMemory(mem);
      }
    }

    // tuple-ids=7 row-size=212B cardinality=1
    // tuple-ids=3,1,5,4,6 row-size=459B cardinality=1

    protected void parseTupleLine(PlanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("tuple-ids=(\\S+) row-size=(\\S+) cardinality=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      String parts[] = m.group(1).split(",");
      node.tupleIds = new int[parts.length];
      for (int i = 0; i < parts.length; i++) {
        node.tupleIds[i] = Integer.parseInt(parts[i]);
      }
      node.estRowSize = (int) Math.round(ParseUtils.parsePlanMemory(m.group(2)));
      node.estCardinality = Integer.parseInt(m.group(3));
    }

    // output: max(a16.wshe_name), max(concat(ifnull(a13.prvsn_commdty_nm, ' '), ...

    private void parseAggOutputLine(AggregateNode node, int posn) {
      String line = node.details.get(posn);
      node.output = ParseUtils.parseExprs(line.substring("output: ".length()));
    }

    // group by: CAST(a11.selling_org_id AS INT), ...

    private void parseAggGroupByLine(AggregateNode node, int posn) {
      String line = node.details.get(posn);
      node.groupBy = ParseUtils.parseExprs(line.substring("group by: ".length()));
    }

    protected int parseJoinDetails(JoinNode node) {
      int posn = 0;
      String line = node.details.get(posn);
      if (line.startsWith("hash predicates:")) {
        parsePredicateLine(node, line);
        line = node.details.get(++posn);
      }
      if (line.startsWith("runtime filters: ")) {
        parseFiltersLine(node, line);
        posn++;
      }
      return posn;
    }

    // hash predicates: CAST(a11.selling_org_id AS INT) = a16.wshe_number, ...

    private void parsePredicateLine(JoinNode node, String line) {
      node.hashPredicates = ParseUtils.parseExprs(line.substring("hash predicates: ".length()));
    }

    // runtime filters: RF001 <- a16.wshe_number, ...

    private void parseFiltersLine(JoinNode node, String line) {
      node.filters = ParseUtils.parseExprs(line.substring("runtime filters: ".length()));
    }

    protected int parseExchangeDetails(ExchangeNode node) {
      return 0;
    }

    protected int parseHdfsDetails(HdfsScanNode node) {
      int posn = 0;
      parsePartitionsLine(node, posn++);
      posn = parsePredicatesLine(node, posn);
      posn = parseFiltersLine(node, posn);
      parseTableStatsLine(node, posn++);
      parseColumnStatsLine(node, posn++);
      return posn;
    }

    // partitions=1/1 files=1 size=6.65KB

    private void parsePartitionsLine(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("partitions=(\\d+)/(\\d+) files=(\\d+) size=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      node.partitionIndex = Integer.parseInt(m.group(1));
      node.partitionCount = Integer.parseInt(m.group(2));
      node.fileCount = Integer.parseInt(m.group(3));
      node.partitionSize = (int) Math.round(ParseUtils.parsePlanMemory(m.group(4)));
    }

    public static String PREDICATES_PREFIX = "predicates: ";

    // predicates: a15.invc_prvsn_commdty_id = 320144, ...

    private int parsePredicatesLine(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      if (! line.startsWith(PREDICATES_PREFIX)) {
        return posn;
      }
      node.predicates = ParseUtils.parseExprs(line.substring(PREDICATES_PREFIX.length()));
      return ++posn;
    }

    public static final String RUNTIME_FILTERS_PREFIX = "runtime filters: ";

    // runtime filters: RF001 -> CAST(a15.selling_org_id AS INT), ...

    private int parseFiltersLine(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      if (! line.startsWith(RUNTIME_FILTERS_PREFIX)) {
        return posn;
      }
      node.filters = ParseUtils.parseExprs(line.substring(RUNTIME_FILTERS_PREFIX.length()));
      return ++posn;
    }

    // table stats: 154 rows total

    private void parseTableStatsLine(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("table stats: (\\d+) rows total");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      node.estRowCount = Integer.parseInt(m.group(1));
    }

    // column stats: all

    private void parseColumnStatsLine(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("column stats: (.*)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      // TODO: Extract more info?
      node.columnStats = m.group(1);
    }
  }

  public static class Ver2_9_0Parser extends Ver2_6_0Parser {

    private PlanFragment currentFragment;

    public Ver2_9_0Parser(QueryPlan plan, String version) {
      super(plan, version);
    }

    @Override
    protected void parsePlan() {
      plan.rootFragment = parseFragment(nextLine());
      Preconditions.checkNotNull(plan.rootFragment);
      plan.fragments = new PlanFragment[plan.rootFragment.fragmentId + 1];
      plan.fragments[plan.rootFragment.fragmentId] = plan.rootFragment;
      parseRootNode(nextLine());
      parseNode(null, 0);
    }

    @Override
    protected void parseNode(PlanNode parent, int depth) {
      String line = nextLine();
      if (parseFragment(line) != null) {
        line = nextLine();
      }
      doParseNode(line, parent, depth);
    }

    // F01:PLAN FRAGMENT [UNPARTITIONED] hosts=1 instances=1

    private PlanFragment parseFragment(String line) {
      Pattern p = Pattern.compile("F(\\d+):PLAN FRAGMENT \\[([^]]+)\\] hosts=(\\d+) instances=(\\d+)");
      Matcher m = p.matcher(line);
      if (! m.matches()) {
        return null;
      }
      int fragmentId = Integer.parseInt(m.group(1));
      int hostCount = Integer.parseInt(m.group(3));
      int instanceCount = Integer.parseInt(m.group(4));
      currentFragment = new PlanFragment(fragmentId, m.group(2), hostCount, instanceCount);
      return currentFragment;
    }

    // PLAN-ROOT SINK
    //   mem-estimate=0B mem-reservation=0B

    private void parseRootNode(String line) {
      Preconditions.checkState(line.equals("PLAN-ROOT SINK"));
      plan.root = new PlanRootNode(this);
      plan.root.details = parseDetailLines(0);
    }

    @Override
    protected void extendNode(PlanNode node) {
      node.fragment = currentFragment;
    }

    @Override
    public void parseDetails(PlanNode node) {
      int posn = parseOperDetails(node);
      parseMemDetails(node, posn++);
      if (! node.isRoot()) {
        parseTupleLine(node, posn++);
      }
    }

    // mem-estimate=0B mem-reservation=0B
    protected void parseMemDetails(PlanNode node, int posn) {
      String line = node.details.get(posn);
      Pattern p = Pattern.compile("mem-estimate=(\\S+) mem-reservation=(.+)");
      Matcher m = p.matcher(line);
      Preconditions.checkState(m.matches());
      node.perHostMemory = ParseUtils.parsePlanMemory(m.group(1));
      node.memReservation = ParseUtils.parsePlanMemory(m.group(2));
    }

    @Override
    protected int parseHdfsDetails(HdfsScanNode node) {
      int posn = super.parseHdfsDetails(node);
      return parseParquetDict(node, posn);
    }

    public static final String PARQUET_DICT_PREFIX = "parquet dictionary predicates: ";

    // parquet dictionary predicates: FCN_PARM_NM = 'EMAIL'
    private int parseParquetDict(HdfsScanNode node, int posn) {
      String line = node.details.get(posn);
      if (! line.startsWith(PARQUET_DICT_PREFIX)) {
        return posn;
      }
      node.parquetDictPredicates = ParseUtils.parseExprs(line.substring(PARQUET_DICT_PREFIX.length()));
      return ++posn;
    }
  }

  private final ProfileFacade profile;
  private final String textPlan;
  private PlanNode[] nodes;
  private PlanFragment rootFragment;
  private PlanFragment fragments[];
  private List<String> rootDetails = new ArrayList<>();
  private List<String> details = new ArrayList<>();
  private QueryPlan.PlanNode root;
  private long perHostMemory;
  private int perHostVCores;
  private boolean hasSummary;
  private boolean parsedTail;
  private boolean planDetailsParsed;

  public QueryPlan(ProfileFacade query) {
    this.profile = query;
    textPlan = query.summary().attrib(Attrib.SummaryAttrib.PLAN);
    try {
      buildPlan();
    } catch (IOException e) {
      // Should never occur
      throw new IllegalStateException(e);
    }
  }

  private void buildPlan() throws IOException {
    System.out.println(profile.fullVersion());
    System.out.println(textPlan);
    PlanParser parser = PlanParser.createParser(this);
    parser.parse();
  }

  public void parseDetails() {
    if (parsedTail) { return; }
    for (int i = 0; i < nodes.length; i++) {
      nodes[i].parseTail();
      nodes[i].parseDetails();
    }
    parsedTail = true;
  }

  public void parseSummary() {
    if (hasSummary) { return; }
    try {
      BufferedReader in = new BufferedReader(
          new StringReader(profile.summary().attrib(
              Attrib.SummaryAttrib.EXEC_SUMMARY)));
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
