package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;

import jersey.repackaged.com.google.common.base.Preconditions;

/**
 * Wrapper around, and analyzer of a query profile. Provides easy access to
 * relevant bits of the profile. Parses the text plan and execution summary.
 * Parses the abstract tree format into a single view of the plan with both
 * plan and execution time information.
 *
 * <h4>Facade Structure</h4>
 *
 * This class provides a higher level, logical structure on top of the
 * lower level physical structure of the profile (as shown below.)
 *
 * The key parts are:
 * <ul>
 * <li>Overview</li>
 * <li>Query Plan
 *   <ul>
 *   <li>Fragment
 *     <ul>
 *     <li>Fragment</li>
 *     <li>Fragment</li>
 *     </ul></li>
 *   <ul></li>
 * <ul>
 *
 * The plan and execution nodes are merged into a single fragment tree.
 * Fragments are available via their index, or via their tree structure.
 *
 * <h4>Tree Overview</h4>
 *
 * Root node
 * <ul>
 * <li>Overview node ("Query (id=...)" (req)</li>
 * <li>Server ("ImpalaServer") (opt)</li>
 * <li>Execution ("Execution Profile ...") (opt)
 *   <ul>
 *   <li>Fragment
 *     <ul>
 *     <li>Fragment</li>
 *     <li>...</li>
 *     </ul></li>
 *   <li>Fragment</li>
 *   <li>...</li>
 *   </ul></li>
 * </ul>
 *
 * The server and execution nodes appear only if the statement succeeded.
 */
public class ProfileAnalyzer {

  public enum QueryType {

    // Values must match the text in the profile
    // TODO: Verify full list

    DDL("DDL"),
    DML("DML"),
    QUERY("QUERY"),
    SET("SET"),
    EXPLAIN("EXPLAIN"),
    UNKNOWN("N/A");

    public final String label;

    private QueryType(String label) {
      this.label = label;
    }

    public String label() { return label; }

    public static QueryType typeFor(String value) {
      if (value == null) {
        return UNKNOWN;
      }
      QueryType values[] = values();
      for (int i = 0; i < values.length; i++) {
        if (values[i].label().equals(value)) {
          return values[i];
        }
      }
      return UNKNOWN;
    }
  }

  public enum SummaryState {
    // TODO: Fill in other. Failure? Cancelled?

    OK, FAILED, OTHER
  }

  public enum QueryState {

    // Names must match text in the profile
    // TODO: Verify full list

    FINISHED;

    public static QueryState typeFor(String value) {
      return QueryState.valueOf(value);
    }
  }

  public static class ProfileNode {

    protected final ProfileAnalyzer analyzer;
    protected final TRuntimeProfileNode node;
    protected final int index;
    protected int firstChild;

    public ProfileNode(ProfileAnalyzer analyzer, int index) {
      this.analyzer = analyzer;
      this.index = index;
      node = analyzer.node(index);
      firstChild = index + 1;
//    for (int i = 0; i < childCount; i++) {
//      TRuntimeProfileNode child = analyzer.node(firstChild + i);
//      System.out.println(child.getName());
//    }
    }

    public String attrib(String key) {
      return node.getInfo_strings().get(key);
    }

    public void generateAttribs() {
      generateAttribs(node);
    }

    public String name() {
      return node.getName();
    }

    public int childCount() {
      return node.getNum_children();
    }

    /**
     * Development method to generate enum names from observed
     * keys in the "info" field order list.
     * @param node the node to parse
     */
    public static void generateAttribs(TRuntimeProfileNode node) {
      System.out.println("// Generated using genAttribs()");
      System.out.println();
      List<String> keys = node.getInfo_strings_display_order();
      int count = keys.size();
      for (int i = 0; i < count; i++) {
        String key = keys.get(i);
        String enumName = key.toUpperCase();
        enumName = enumName.replaceAll("[ \\-()]", "_");
        enumName = enumName.replaceAll("__", "_");
        enumName = enumName.replaceAll("_$", "");
        String term = (i + 1 == count) ? ";" : ",";
        System.out.println(String.format("%s(\"%s\")%s",
            enumName, key, term));
      }
    }
  }

  public static class RootNode extends ProfileNode {

    public static String IMPALA_SERVER_NODE = "ImpalaServer";
    public static String EXEC_PROFILE_NODE = "Execution Profile";

    private final SummaryNode summaryNode;
    private HelperNode serverNode;
    private ExecProfileNode execNode;
    public String queryId;

    public RootNode(ProfileAnalyzer analyzer) {
      super(analyzer, 0);
      Preconditions.checkState(childCount() == 1 || childCount() == 3);
      summaryNode = new SummaryNode(analyzer, 1);
      if (childCount() == 3) {
        serverNode = new HelperNode(analyzer, 2);
        execNode = new ExecProfileNode(analyzer, 3);
      }
      Pattern p = Pattern.compile("\\(id=([^)]+)\\)");
      Matcher m = p.matcher(name());
      if (m.find()) {
        queryId = m.group(1);
      }
    }

    public SummaryNode summary() { return summaryNode; }
    public HelperNode serverNode() { return serverNode; }
    public ExecProfileNode execNode() { return execNode; }
    public String queryId() { return queryId; }
  }

  public static class HelperNode extends ProfileNode {

    public HelperNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
      Preconditions.checkState(childCount() == 0);
    }
  }

  public static class NodeIndex {
    int index;

    public NodeIndex(int index) {
      this.index = index;
    }
  }

  public static class ExecProfileNode extends ProfileNode {

    public boolean expanded;
    private FragmentExecNode coordinator;
    private final List<FragmentExecNode> summaries = new ArrayList<>();
    private final List<InstancesNode> details = new ArrayList<>();

    public ExecProfileNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public void expand() {
      if (expanded) { return; }
      NodeIndex index = new NodeIndex(firstChild);
      for (int i = 0; i < childCount(); i++) {
        TRuntimeProfileNode profileNode = analyzer.node(index.index);
        String name = profileNode.getName();
        if (name.startsWith(FragmentExecNode.COORD_PREFIX)) {
          coordinator = new FragmentExecNode(analyzer, index,
              FragmentExecNode.FragmentType.COORDINATOR);
        } else  if (name.startsWith(InstancesNode.NAME_PREFIX)) {
          details.add(new InstancesNode(analyzer, index));
        } else if (name.startsWith(FragmentExecNode.AVERAGED_PREFIX)) {
          summaries.add(new FragmentExecNode(analyzer, index,
              FragmentExecNode.FragmentType.AVERAGED));
        } else {
          throw new IllegalStateException("Exec node type");
        }
      }
      expanded = true;
    }

    public FragmentExecNode coordinator() {
      return coordinator;
    }

    public List<FragmentExecNode> summaries() {
      return summaries;
    }

    public List<InstancesNode> details() {
      return details;
    }
  }

  /**
   * Represents the statement as a whole. Has three children:
   * <ol>
   * <li>Summary (req)</li>
   * <li>ImpalaServer (opt)</li>
   * <li>Execution Profile (opt)</li>
   * </ol>
   */
  public static class SummaryNode extends ProfileNode {

    public static String FINISHED_STATE = "FINISHED";
    public static String EXCEPTION_STATE = "EXCEPTION";
    public static String OK_STATUS = "OK";

    public enum Attrib {

      // Generated using genAttribs()
      // Listed in display order as defined in client-request-state.cpp

      SESSION_ID("Session ID"),
      SESSION_TYPE("Session Type"),
      HIVE_SERVER_2_PROTOCOL_VERSION("HiveServer2 Protocol Version"),
      START_TIME("Start Time"),
      END_TIME("End Time"),
      QUERY_TYPE("Query Type"),
      QUERY_STATE("Query State"),
      QUERY_STATUS("Query Status"),
      IMPALA_VERSION("Impala Version"),
      USER("User"),
      CONNECTED_USER("Connected User"),
      DELEGATED_USER("Delegated User"),
      NETWORK_ADDRESS("Network Address"),
      DEFAULT_DB("Default Db"),
      SQL_STATEMENT("Sql Statement"),
      COORDINATOR("Coordinator"),
      QUERY_OPTIONS_NON_DEFAULT("Query Options (non default)"),
      DDL_TYPE("DDL Type"),
      PLAN("Plan"),
      ESTIMATED_PER_HOST_MEM("Estimated Per-Host Mem"),
      ESTIMATED_PER_HOST_VCORES("Estimated Per-Host VCores"),
      REQUEST_POOL("Request Pool"),
      ADMISSION_RESULT("Admission result"),
      EXECSUMMARY("ExecSummary");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    public SummaryNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public SummaryState summaryState() {
      String state = attrib(Attrib.QUERY_STATE);
      if (state.equals(EXCEPTION_STATE)) {
        return SummaryState.FAILED;
      }
      if (! state.equals(FINISHED_STATE)) {
        return SummaryState.OTHER;
      }
      String status = attrib(Attrib.QUERY_STATUS);
      if (status.equals(OK_STATUS)) {
        return SummaryState.OK;
      }
      return SummaryState.FAILED;
    }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public QueryType type() {
      try {
        return QueryType.typeFor(attrib(Attrib.QUERY_TYPE));
      } catch (Exception e) {
        return null;
      }
    }
  }

  /**
   * Parent class for the three kinds of fragment nodes:
   * <pre>
   * Coordinator Fragment F10
   * Averaged Fragment F09
   * Fragment F08:
   *   Instance ...
   * <pre>
   *
   * Note that the third kind is not really a fragment, rather
   * it is a container of per-host fragment details.
   */
  public static class ExecNode extends ProfileNode {
    protected int fragmentId;

    public ExecNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public int fragmentId() { return fragmentId; }
  }

  /**
   * Details for a fragment. Holds a list of fragment details,
   * one per host.
   *
   * <pre>
   * FRAGMENT F08:
   *   Instance xxx:xxx (host=xxx)
   *   ...
   * </pre>
   */
  public static class InstancesNode extends ExecNode {

    public static final String NAME_PREFIX = "Fragment ";

    protected final List<FragmentExecNode> fragments = new ArrayList<>();

    public InstancesNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index.index);
      index.index++;
      Pattern p = Pattern.compile("Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
      for (int i = 0; i < childCount(); i++) {
        FragmentExecNode frag = new FragmentExecNode(analyzer, index, fragmentId);
        fragments.add(frag);
      }
    }

    public List<FragmentExecNode> hostNodes() {
      return fragments;
    }
  }

  /**
   * Execution detail for a single fragment or an average over
   * a set of fragments.
   *
   * <pre>
   * Coordinator Fragment | Averaged Fragment | Instance
   *   BlockMgr
   *   CodeGen
   *   DataStreamSender
   *   &lt;OPERATOR>_NODE
   *   Filter
   * </pre>
   */
  public static class FragmentExecNode extends ExecNode {

    public static final String AVERAGED_PREFIX = "Averaged ";
    public static final String COORD_PREFIX = "Coordinator ";

    public enum FragmentType {
      COORDINATOR,
      AVERAGED,
      INSTANCE
    }

    protected final FragmentType fragmentType;
    protected String fragmentGuid;
    protected String serverId;
    protected BlockMgrNode blockMgr;
    protected final List<OperatorExecNode> operators = new ArrayList<>();
    private CodeGenNode codeGen;
    private DataStreamSenderNode dataStreamSender;

    public FragmentExecNode(ProfileAnalyzer analyzer, NodeIndex index, FragmentType fragmentType) {
      super(analyzer, index.index++);
      this.fragmentType = fragmentType;
      Pattern p = Pattern.compile(".*Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
      parseChildren(index);
    }

    public FragmentExecNode(ProfileAnalyzer analyzer, NodeIndex index, int fragmentId) {
      super(analyzer, index.index++);
      this.fragmentId = fragmentId;
      fragmentType = FragmentType.INSTANCE;
      Pattern p = Pattern.compile("Instance (\\S+) \\(host=([^(]+)\\)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentGuid = m.group(1);
      serverId = m.group(2);
      parseChildren(index);
    }

    private void parseChildren(NodeIndex index) {
      Preconditions.checkState(index.index == firstChild);
      for (int i = 0; i < childCount(); i++) {
        TRuntimeProfileNode profileNode = analyzer.node(index.index);
        String name = profileNode.getName();
        parseChild(name, index);
      }
    }

    protected void parseChild(String name, NodeIndex index) {
      if (name.equals(BlockMgrNode.NAME)) {
        blockMgr = new BlockMgrNode(analyzer, index.index++);
      } else if (name.equals(CodeGenNode.NAME)) {
        codeGen = new CodeGenNode(analyzer, index.index++);
      } else if (name.startsWith(DataStreamSenderNode.NAME_PREFIX)) {
        dataStreamSender = new DataStreamSenderNode(analyzer, index.index++);
      } else  {
        operators.add(OperatorExecNode.parseOperator(analyzer, name, index));
      }
    }

    public String serverId() { return serverId; }

    public List<OperatorExecNode> operators() { return operators; }
  }

  /**
   * Represents execution-time detail about an operator.
   *
   * <pre>
   * &lt;operator>_NODE (id=xx)
   *   &lt;operator>_NODE (id=xx)
   *   ...
   *   filter
   *   ...
   * </pre>
   *
   * The child operators represent the inputs to a binary
   * operator (such as a join).
   */
  public static class OperatorExecNode extends ProfileNode {

    private final String operatorName;
    private final int operatorIndex;
    private List<OperatorExecNode> children = new ArrayList<>();
    private final List<FilterNode> filters = new ArrayList<>();

    public OperatorExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index.index++);
      Pattern p = Pattern.compile("(.*)_NODE \\(id=(\\d+)\\)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      operatorName = m.group(1);
      operatorIndex = Integer.parseInt(m.group(2));
      for (int i = 0; i < childCount(); i++) {
        TRuntimeProfileNode profileNode = analyzer.node(index.index);
        String name = profileNode.getName();
        if (name.startsWith(FilterNode.NAME_PREFIX)) {
          filters.add(new FilterNode(analyzer, index.index++));
        } else {
          children.add(parseOperator(analyzer, name, index));
        }
      }
    }

    public static OperatorExecNode parseOperator(
        ProfileAnalyzer analyzer, String name, NodeIndex index) {
      if (name.startsWith(ExchangeExecNode.NAME_PREFIX)) {
        return new ExchangeExecNode(analyzer, index);
      } else if (name.startsWith(AggExecNode.NAME_PREFIX)) {
        return new AggExecNode(analyzer, index);
      } else if (name.startsWith(HashJoinExecNode.NAME_PREFIX)) {
        return new HashJoinExecNode(analyzer, index);
      } else if (name.startsWith(HdfsScanExecNode.NAME_PREFIX)) {
        return new HdfsScanExecNode(analyzer, index);
      } else {
        throw new IllegalStateException("Operator type: " + name);
      }
    }

    public int operatorId() { return operatorIndex; }

    public List<OperatorExecNode> children() {
      return children;
    }
  }

  public static class ExchangeExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "EXCHANGE_NODE ";

    public ExchangeExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }
  }

  public static class AggExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "AGGREGATION_NODE ";

    public AggExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }
  }

  public static class HashJoinExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "HASH_JOIN_NODE ";

    public HashJoinExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }
  }

  public static class HdfsScanExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "HDFS_SCAN_NODE ";

    public HdfsScanExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }
  }

  public static class CodeGenNode extends HelperNode {

    public static final String NAME = "CodeGen";

    public CodeGenNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }
  }

  public static class DataStreamSenderNode extends HelperNode {

    public static final String NAME_PREFIX = "DataStreamSender ";

    public DataStreamSenderNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }
  }

  public static class FilterNode extends HelperNode {

    public static String NAME_PREFIX = "Filter ";

    public FilterNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }
  }

  public static class BlockMgrNode extends HelperNode {

    public static final Object NAME = "BlockMgr";

    public BlockMgrNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }
  }

  private final String queryId;
  private final String label;
  private final TRuntimeProfileTree profile;
  private final RootNode root;
  private final SummaryNode summary;
  private QueryPlan plan;
  private QueryDAG dag;


  public ProfileAnalyzer(TRuntimeProfileTree profile) {
    this(profile, null, null);
  }

  public ProfileAnalyzer(TRuntimeProfileTree profile,
      String queryId, String label) {
    this.queryId = queryId;
    this.label = label;
    this.profile = profile;
    root = new RootNode(this);
    summary = root.summary();
    Preconditions.checkArgument(queryId == null ||
        queryId.equals(root.queryId()));
  }

  public TRuntimeProfileNode node(int i) {
    return profile.nodes.get(i);
  }

  public String queryId() {
    return queryId == null ? root.queryId() : queryId;
  }

  public String label() { return label; }

  public String title() {
    StringBuilder buf = new StringBuilder();
    if (label != null) {
      buf
        .append(label)
        .append(": ");
    }
    buf
      .append(summary.type().name())
      .append(" (")
      .append(summary.summaryState().name())
      .append(")")
      .append(" - ID = ")
      .append(queryId());
    return buf.toString();
  }

  public SummaryNode summary() { return summary; }

  public String stmt() {
    return summary.attrib(SummaryNode.Attrib.SQL_STATEMENT);
  }

  public QueryPlan plan() {
    if (plan == null) {
      plan = new QueryPlan(summary);
    }
    return plan;
  }

  public void computePlanSummary() {
    plan().parseSummary();
    plan().parseTail();
  }

  public void parsePlanDetails() {
    plan().parsePlanDetails();
  }

  public void expandExecNodes() {
    root.execNode().expand();
  }

  public ExecProfileNode exec() {
    expandExecNodes();
    return root.execNode();
  }

  public void buildDag() {
    if (dag != null) { return; }
    dag = new QueryDAG(this);
  }

  public QueryDAG dag() {
    buildDag();
    return dag;
  }
}
