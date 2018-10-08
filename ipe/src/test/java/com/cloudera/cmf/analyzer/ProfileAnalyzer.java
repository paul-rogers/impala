package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TTimeSeriesCounter;
import org.apache.impala.thrift.TUnit;

import com.google.common.collect.Lists;

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
 *
 * <h4>Attributes and Counters</h4>
 *
 * Nodes provide attributes (called "info strings") and counters. Very
 * few nodes have attributes. For those that do, the node facade classes
 * define enums to identify the attributes available. Since attributes
 * tend to have rather long names, the enums provide an easy way to
 * ensure the correct name is used.
 * <p>
 * Execution nodes have counters. Enums exist for each of these, and the
 * enum provides the units used for the counter (the same information
 * in the tree itself, but enums provide compile-time access to the
 * units.) The same is true for time series counters.
 * <p>
 * The Thrift schema defines summary counters
 * (<code>TSummaryStatsCounter</code), but it appears that they
 * are not actually used in the profile.
 * <p>
 * Each node defines an event sequence, but this attribute is used
 * in only one node: the summary node.
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
    }

    public TRuntimeProfileNode node() { return node; }

    public String attrib(String key) {
      return node.getInfo_strings().get(key);
    }

    public String name() {
      return node.getName();
    }

    public int childCount() {
      return node.getNum_children();
    }

    public String genericName() {
      return node.getName();
    }

    public List<ProfileNode> childNodes() {
      return Lists.newArrayList();
    }

    public long counter(String name) {
      // TODO: Cache counters in a map?
      for (TCounter counter : node.getCounters()) {
        if (counter.getName().equals(name)) {
          return counter.getValue();
        }
      }
      return 0;
    }

    public TEventSequence events(String name) {
      // Used in only one node, only two sequences.
      // Linear search is fine.
      for (TEventSequence event : node.getEvent_sequences()) {
        if (event.getName().equals(name)) {
          return event;
        }
      }
      return null;
    }

    public TTimeSeriesCounter timeSeries(String name) {
      // Used in only a few nodes, only two sequences.
      // Linear search is fine.
      for (TTimeSeriesCounter event : node.getTime_series_counters()) {
        if (event.getName().equals(name)) {
          return event;
        }
      }
      return null;
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
    private CoordinatorExecNode coordinator;
    private final List<FragSummaryExecNode> summaries = new ArrayList<>();
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
        if (name.startsWith(CoordinatorExecNode.PREFIX)) {
          coordinator = new CoordinatorExecNode(analyzer, index);
        } else  if (name.startsWith(InstancesNode.NAME_PREFIX)) {
          details.add(new InstancesNode(analyzer, index));
        } else if (name.startsWith(FragSummaryExecNode.PREFIX)) {
          summaries.add(new FragSummaryExecNode(analyzer, index));
        } else {
          throw new IllegalStateException("Exec node type");
        }
      }
      expanded = true;
    }

    public CoordinatorExecNode coordinator() {
      return coordinator;
    }

    public List<FragSummaryExecNode> summaries() {
      return summaries;
    }

    public List<InstancesNode> details() {
      return details;
    }

    @Override
    public String genericName() { return "Execution Profile"; }

    @Override
    public List<ProfileNode> childNodes() {
      List<ProfileNode> children = new ArrayList<>();
      children.add(coordinator);
      children.addAll(summaries);
      children.addAll(details);
      return children;
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
      EXEC_SUMMARY("ExecSummary");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    public enum EventSequence {
      SESSION_TYPE("Planner Timeline"),
      QUERY_TIMELINE("Query Timeline");

      private final String key;

      private EventSequence(String key) {
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

    public TEventSequence events(EventSequence attrib) {
      return events(attrib.key());
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

    // Generated using EnumBuilder
    public enum Attrib {
      NUMBER_OF_FILTERS("Number of filters"),
      FILTER_ROUTING_TABLE("Filter routing table"),
      FRAGMENT_START_LATENCIES("Fragment start latencies"),
      FINAL_FILTER_TABLE("Final filter table"),
      PER_NODE_PEAK_MEMORY_USAGE("Per Node Peak Memory Usage");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      FILTERS_RECEIVED("FiltersReceived", TUnit.UNIT),
      FINALIZATION_TIMER("FinalizationTimer", TUnit.TIME_NS),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    protected int fragmentId;

    public ExecNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public int fragmentId() { return fragmentId; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
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

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      MEMORY_USAGE("MemoryUsage", TUnit.BYTES),
      THREAD_USAGE("ThreadUsage", TUnit.UNIT);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    protected final List<FragInstanceExecNode> fragments = new ArrayList<>();

    public InstancesNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index.index);
      index.index++;
      Pattern p = Pattern.compile("Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
      for (int i = 0; i < childCount(); i++) {
        FragInstanceExecNode frag = new FragInstanceExecNode(analyzer, index, fragmentId);
        fragments.add(frag);
      }
    }

    public List<FragInstanceExecNode> hostNodes() {
      return fragments;
    }

    @Override
    public String genericName() { return "Fragment"; }

    @Override
    public List<ProfileNode> childNodes() {
      return Lists.newArrayList(fragments);
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
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
  public static abstract class FragmentExecNode extends ExecNode {

    public enum FragmentType {
      COORDINATOR,
      AVERAGED,
      INSTANCE
    }

    protected BlockMgrNode blockMgr;
    protected final List<OperatorExecNode> operators = new ArrayList<>();
    private CodeGenNode codeGen;
    private DataStreamSenderNode dataStreamSender;

    public FragmentExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index.index++);
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

    public List<OperatorExecNode> operators() { return operators; }

    public abstract FragmentType fragmentType();

    @Override
    public String genericName() { return fragmentType().name(); }

    @Override
    public List<ProfileNode> childNodes() {
      List<ProfileNode> children = new ArrayList<>();
      if (blockMgr != null) { children.add(blockMgr); }
      if (codeGen != null) { children.add(codeGen); }
      if (dataStreamSender != null) { children.add(dataStreamSender); }
      children.addAll(operators);
      return children;
    }
  }

  public static class CoordinatorExecNode extends FragmentExecNode {

    public static final String PREFIX = "Coordinator ";

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      MEMORY_USAGE("MemoryUsage", TUnit.BYTES),
      THREAD_USAGE("ThreadUsage", TUnit.UNIT);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public CoordinatorExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
      Pattern p = Pattern.compile(PREFIX + "Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
     }

    @Override
    public FragmentType fragmentType() { return FragmentType.COORDINATOR; }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  public static class FragSummaryExecNode extends FragmentExecNode {

    public static final String PREFIX = "Averaged ";

    // Generated using EnumBuilder
    public enum Attrib {
      SPLIT_SIZES("split sizes"),
      COMPLETION_TIMES("completion times"),
      EXECUTION_RATES("execution rates"),
      NUM_INSTANCES("num instances");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public FragSummaryExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
      Pattern p = Pattern.compile(PREFIX + "Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
    }

    @Override
    public FragmentType fragmentType() { return FragmentType.AVERAGED; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  public static class FragInstanceExecNode extends FragmentExecNode {

    // Generated using EnumBuilder
    public enum Attrib {
      HDFS_SPLIT_STATS("Hdfs split stats (<volume id>:<# splits>/<split lengths>)");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    protected String fragmentGuid;
    protected String serverId;

    public FragInstanceExecNode(ProfileAnalyzer analyzer, NodeIndex index, int fragmentId) {
      super(analyzer, index);
      this.fragmentId = fragmentId;
      Pattern p = Pattern.compile("Instance (\\S+) \\(host=([^(]+)\\)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentGuid = m.group(1);
      serverId = m.group(2);
    }

    @Override
    public FragmentType fragmentType() {
      return FragmentType.INSTANCE;
    }

    public String serverId() { return serverId; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public String filterArrival(int filterNo) {
      return attrib("Filter " + filterNo + " arrival");
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
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

    @Override
    public String genericName() { return operatorName; }

    @Override
    public List<ProfileNode> childNodes() {
      List<ProfileNode> children = new ArrayList<>();
      children.addAll(children);
      children.addAll(filters);
      return children;
    }
  }

  public static class ExchangeExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "EXCHANGE_NODE ";

    // Generated using EnumBuilder
    public enum Counter {
      BYTES_RECEIVED("BytesReceived", TUnit.BYTES),
      CONVERT_ROW_BATCH_TIME("ConvertRowBatchTime", TUnit.TIME_NS),
      DESERIALIZE_ROW_BATCH_TIMER("DeserializeRowBatchTimer", TUnit.TIME_NS),
      FIRST_BATCH_ARRIVAL_WAIT_TIME("FirstBatchArrivalWaitTime", TUnit.TIME_NS),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      SENDERS_BLOCKED_TIMER("SendersBlockedTimer", TUnit.TIME_NS),
      SENDERS_BLOCKED_TOTAL_TIMER("SendersBlockedTotalTimer(*)", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

 // Generated using EnumBuilder
    public enum TimeSeries {
      BYTES_RECEIVED("BytesReceived", TUnit.BYTES);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public ExchangeExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  /**
   * Facade for AGGREGATION nodes, both the STREAMING and FINALIZE
   * variants. The two types have distinct counters, they their
   * execution nodes have the same name. The plan indicates which
   * type this node represents (match op the operator IDs).
   */
  public static class AggExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "AGGREGATION_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum StreamingCounter {
      GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
      GET_RESULTS_TIME("GetResultsTime", TUnit.TIME_NS),
      HTRESIZE_TIME("HTResizeTime", TUnit.TIME_NS),
      HASH_BUCKETS("HashBuckets", TUnit.UNIT),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
      PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PIN_TIME("PinTime", TUnit.TIME_NS),
      REDUCTION_FACTOR_ESTIMATE("ReductionFactorEstimate", TUnit.DOUBLE_VALUE),
      REDUCTION_FACTOR_THRESHOLD_TO_EXPAND("ReductionFactorThresholdToExpand", TUnit.DOUBLE_VALUE),
      ROWS_PASSED_THROUGH("RowsPassedThrough", TUnit.UNIT),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      STREAMING_TIME("StreamingTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS),
      UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private StreamingCounter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum FinalizeCounter {
      BUILD_TIME("BuildTime", TUnit.TIME_NS),
      GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
      GET_RESULTS_TIME("GetResultsTime", TUnit.TIME_NS),
      HTRESIZE_TIME("HTResizeTime", TUnit.TIME_NS),
      HASH_BUCKETS("HashBuckets", TUnit.UNIT),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
      MAX_PARTITION_LEVEL("MaxPartitionLevel", TUnit.UNIT),
      NUM_REPARTITIONS("NumRepartitions", TUnit.UNIT),
      PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PIN_TIME("PinTime", TUnit.TIME_NS),
      ROWS_REPARTITIONED("RowsRepartitioned", TUnit.UNIT),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      SPILLED_PARTITIONS("SpilledPartitions", TUnit.UNIT),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS),
      UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private FinalizeCounter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public AggExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(FinalizeCounter counter) {
      return counter(counter.key());
    }

    public long counter(StreamingCounter counter) {
      return counter(counter.key());
    }
  }

  public static class HashJoinExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "HASH_JOIN_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      BUILD_PARTITION_TIME("BuildPartitionTime", TUnit.TIME_NS),
      BUILD_ROWS("BuildRows", TUnit.UNIT),
      BUILD_ROWS_PARTITIONED("BuildRowsPartitioned", TUnit.UNIT),
      BUILD_TIME("BuildTime", TUnit.TIME_NS),
      GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
      HASH_BUCKETS("HashBuckets", TUnit.UNIT),
      HASH_COLLISIONS("HashCollisions", TUnit.UNIT),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
      LOCAL_TIME("LocalTime", TUnit.TIME_NS),
      MAX_PARTITION_LEVEL("MaxPartitionLevel", TUnit.UNIT),
      NUM_REPARTITIONS("NumRepartitions", TUnit.UNIT),
      PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PIN_TIME("PinTime", TUnit.TIME_NS),
      PROBE_ROWS("ProbeRows", TUnit.UNIT),
      PROBE_ROWS_PARTITIONED("ProbeRowsPartitioned", TUnit.UNIT),
      PROBE_TIME("ProbeTime", TUnit.TIME_NS),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      SPILLED_PARTITIONS("SpilledPartitions", TUnit.UNIT),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS),
      UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public HashJoinExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  public static class HdfsScanExecNode extends OperatorExecNode {

    public static final String NAME_PREFIX = "HDFS_SCAN_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption"),
      HDFS_SPLIT_STATS("Hdfs split stats (<volume id>:<# splits>/<split lengths>)"),
      RUNTIME_FILTERS("Runtime filters"),
      HDFS_READ_THREAD_CONCURRENCY_BUCKET("Hdfs Read Thread Concurrency Bucket"),
      FILE_FORMATS("File Formats");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_HDFS_READ_THREAD_CONCURRENCY("AverageHdfsReadThreadConcurrency", TUnit.DOUBLE_VALUE),
      AVERAGE_SCANNER_THREAD_CONCURRENCY("AverageScannerThreadConcurrency", TUnit.DOUBLE_VALUE),
      BYTES_READ("BytesRead", TUnit.BYTES),
      BYTES_READ_DATA_NODE_CACHE("BytesReadDataNodeCache", TUnit.BYTES),
      BYTES_READ_LOCAL("BytesReadLocal", TUnit.BYTES),
      BYTES_READ_REMOTE_UNEXPECTED("BytesReadRemoteUnexpected", TUnit.BYTES),
      BYTES_READ_SHORT_CIRCUIT("BytesReadShortCircuit", TUnit.BYTES),
      DECOMPRESSION_TIME("DecompressionTime", TUnit.TIME_NS),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      MATERIALIZE_TUPLE_TIME("MaterializeTupleTime(*)", TUnit.TIME_NS),
      MAX_COMPRESSED_TEXT_FILE_LENGTH("MaxCompressedTextFileLength", TUnit.BYTES),
      NUM_COLUMNS("NumColumns", TUnit.UNIT),
      NUM_DISKS_ACCESSED("NumDisksAccessed", TUnit.UNIT),
      NUM_ROW_GROUPS("NumRowGroups", TUnit.UNIT),
      NUM_SCANNER_THREADS_STARTED("NumScannerThreadsStarted", TUnit.UNIT),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_READ_THREAD_RAW_HDFS_THROUGHPUT("PerReadThreadRawHdfsThroughput", TUnit.BYTES_PER_SECOND),
      REMOTE_SCAN_RANGES("RemoteScanRanges", TUnit.UNIT),
      ROWS_READ("RowsRead", TUnit.UNIT),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      SCAN_RANGES_COMPLETE("ScanRangesComplete", TUnit.UNIT),
      SCANNER_THREADS_INVOLUNTARY_CONTEXT_SWITCHES("ScannerThreadsInvoluntaryContextSwitches", TUnit.UNIT),
      SCANNER_THREADS_SYS_TIME("ScannerThreadsSysTime", TUnit.TIME_NS),
      SCANNER_THREADS_TOTAL_WALL_CLOCK_TIME("ScannerThreadsTotalWallClockTime", TUnit.TIME_NS),
      SCANNER_THREADS_USER_TIME("ScannerThreadsUserTime", TUnit.TIME_NS),
      SCANNER_THREADS_VOLUNTARY_CONTEXT_SWITCHES("ScannerThreadsVoluntaryContextSwitches", TUnit.UNIT),
      TOTAL_RAW_HDFS_READ_TIME("TotalRawHdfsReadTime(*)", TUnit.TIME_NS),
      TOTAL_READ_THROUGHPUT("TotalReadThroughput", TUnit.BYTES_PER_SECOND),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      BYTES_READ("BytesRead", TUnit.BYTES);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public HdfsScanExecNode(ProfileAnalyzer analyzer, NodeIndex index) {
      super(analyzer, index);
    }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  public static class CodeGenNode extends HelperNode {

    public static final String NAME = "CodeGen";

    // Generated using EnumBuilder
    public enum Counter {
      CODEGEN_TIME("CodegenTime", TUnit.TIME_NS),
      COMPILE_TIME("CompileTime", TUnit.TIME_NS),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      LOAD_TIME("LoadTime", TUnit.TIME_NS),
      MODULE_BITCODE_SIZE("ModuleBitcodeSize", TUnit.BYTES),
      NUM_FUNCTIONS("NumFunctions", TUnit.UNIT),
      NUM_INSTRUCTIONS("NumInstructions", TUnit.UNIT),
      OPTIMIZATION_TIME("OptimizationTime", TUnit.TIME_NS),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public CodeGenNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  public static class DataStreamSenderNode extends HelperNode {

    // Generated using EnumBuilder
    public enum Counter {
      BYTES_SENT("BytesSent", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      NETWORK_THROUGHPUT("NetworkThroughput(*)", TUnit.BYTES_PER_SECOND),
      OVERALL_THROUGHPUT("OverallThroughput", TUnit.BYTES_PER_SECOND),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      SERIALIZE_BATCH_TIME("SerializeBatchTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS),
      TRANSMIT_DATA_RPCTIME("TransmitDataRPCTime", TUnit.TIME_NS),
      UNCOMPRESSED_ROW_BATCH_SIZE("UncompressedRowBatchSize", TUnit.BYTES);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }
    public static final String NAME_PREFIX = "DataStreamSender ";

    public DataStreamSenderNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public String genericName() { return NAME_PREFIX.trim(); }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  public static class FilterNode extends HelperNode {

    public static String NAME_PREFIX = "Filter ";

    // Generated using EnumBuilder
    public enum Counter {
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      ROWS_PROCESSED("Rows processed", TUnit.UNIT),
      ROWS_REJECTED("Rows rejected", TUnit.UNIT),
      ROWS_TOTAL("Rows total", TUnit.UNIT),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public FilterNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public String genericName() { return NAME_PREFIX.trim(); }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  public static class BlockMgrNode extends HelperNode {

    public static final Object NAME = "BlockMgr";

    // Generated using EnumBuilder
    public enum Counter {
      BLOCK_WRITES_OUTSTANDING("BlockWritesOutstanding", TUnit.UNIT),
      BLOCKS_CREATED("BlocksCreated", TUnit.UNIT),
      BLOCKS_RECYCLED("BlocksRecycled", TUnit.UNIT),
      BUFFERED_PINS("BufferedPins", TUnit.UNIT),
      BYTES_WRITTEN("BytesWritten", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      MAX_BLOCK_SIZE("MaxBlockSize", TUnit.BYTES),
      MEMORY_LIMIT("MemoryLimit", TUnit.BYTES),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      TOTAL_BUFFER_WAIT_TIME("TotalBufferWaitTime", TUnit.TIME_NS),
      TOTAL_ENCRYPTION_TIME("TotalEncryptionTime", TUnit.TIME_NS),
      TOTAL_INTEGRITY_CHECK_TIME("TotalIntegrityCheckTime", TUnit.TIME_NS),
      TOTAL_READ_BLOCK_TIME("TotalReadBlockTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public BlockMgrNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }

    public long counter(Counter counter) {
      return counter(counter.key());
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
