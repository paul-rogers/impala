package com.cloudera.cmf.profile;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileTree;

import com.cloudera.cmf.profile.ProfileNode.NodeIterator;
import com.google.common.base.Preconditions;

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
 *   <li>Coordinator</li>
 *   <li>Averaged Fragment</li>
 *   <li>...</li>
 *   <li>Fragment</li><ul>
 *     <li>Instance</li>
 *     <li>...</li>
 *     </ul></li>
 *   <li>...</li>
 *   </ul></li>
 * </ul>
 *
 * Coordinator, Averaged Fragment and Instance nodes have a similar
 * substructure:
 *
 * <ul>
 *   <li>CodeGen</li>
 *   <li>DataStreamSender</li>
 *   <li>BlockMgr</li>
 *   <li><i>Operator</i></ul>
 *     <li><i>Operator</i><li>
 *     <li>...</li>
 *     <li>Filter</li>
 *     </ul></li>
 *   <li>...</li>
 * </ul>
 *
 * The server and execution nodes appear only if the statement succeeded.
 *
 * <h4>Attributes and Counters</h4>
 *
 * Attribute and counter names are defined in the {@link Attrib} class.
 * The classes and enums there provide a structured way to know which
 * names are used by which nodes.
 * <p>
 * Nodes provide attributes (called "info strings"). Very
 * few nodes have attributes.
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
 * Only the summary node defines an event sequence.
 */
public class ProfileFacade {


  public static String FINISHED_STATE = "FINISHED";
  public static String EXCEPTION_STATE = "EXCEPTION";
  public static String OK_STATUS = "OK";

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

  private final String queryId;
  private final String label;
  private final TRuntimeProfileTree profile;
  private final ProfileNode rootNode;
  private final ProfileNode summaryNode;
  private ProfileNode serverNode;
  private ExecPNode execNode;
  private QueryPlan plan;
  private QueryDAG dag;
  private long startTimestamp;
  private long endTimestamp;
  private String version;

  public ProfileFacade(TRuntimeProfileTree profile) {
    this(profile, null, null);
  }

  public ProfileFacade(TRuntimeProfileTree profile,
      String queryId, String label) {
    this.label = label;
    this.profile = profile;
    NodeIterator nodeIndex = new NodeIterator(profile, 0);
    rootNode = new ProfileNode(nodeIndex, PNodeType.ROOT);
    summaryNode = new ProfileNode(nodeIndex, PNodeType.SUMMARY);
    if (rootNode.childCount() == 3) {
      serverNode = new ProfileNode(nodeIndex, PNodeType.SERVER);
      execNode = new ExecPNode(nodeIndex);
    } else {
      serverNode = null;
      execNode = null;
    }
    Pattern p = Pattern.compile("\\(id=([^)]+)\\)");
    Matcher m = p.matcher(rootNode.name());
    Preconditions.checkState(m.find());
    this.queryId = m.group(1);
    Preconditions.checkArgument(queryId == null ||
        queryId.equals(this.queryId));
  }

  public TRuntimeProfileTree profile() {
    return profile;
  }

  public String queryId() {
    return queryId;
  }

  public QueryType type() {
    try {
      return QueryType.typeFor(
          summaryNode.attrib(Attrib.SummaryAttrib.QUERY_TYPE));
    } catch (Exception e) {
      return null;
    }
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
      .append(type().name())
      .append(" (")
      .append(summaryState().name())
      .append(")")
      .append(" - ID = ")
      .append(queryId());
    return buf.toString();
  }

  public SummaryState summaryState() {
    String state = summaryNode.attrib(Attrib.SummaryAttrib.QUERY_STATE);
    if (state.equals(EXCEPTION_STATE)) {
      return SummaryState.FAILED;
    }
    if (! state.equals(FINISHED_STATE)) {
      return SummaryState.OTHER;
    }
    String status = summaryNode.attrib(Attrib.SummaryAttrib.QUERY_STATUS);
    if (status.equals(OK_STATUS)) {
      return SummaryState.OK;
    }
    return SummaryState.FAILED;
  }

  public long durationMs() {
    return endTimestamp() - startTimestamp();
  }

  public long startTimestamp() {
    if (startTimestamp == 0) {
      startTimestamp = ParseUtils.parseStartEndTimestamp(
          summaryNode.attrib(Attrib.SummaryAttrib.START_TIME));
    }
    return startTimestamp;
  }

  public long endTimestamp() {
    if (endTimestamp == 0) {
      endTimestamp = ParseUtils.parseStartEndTimestamp(
          summaryNode.attrib(Attrib.SummaryAttrib.END_TIME));
    }
    return endTimestamp;
  }

  public ProfileNode root() { return rootNode; }
  public ProfileNode summary() { return summaryNode; }
  public ProfileNode server() { return serverNode; }

  public String stmt() {
    return summaryNode.attrib(Attrib.SummaryAttrib.SQL_STATEMENT);
  }

  public String fullVersion() {
    return summaryNode.attrib(Attrib.SummaryAttrib.IMPALA_VERSION);
  }

  public String version() {
    if (version == null) {
      Pattern p = Pattern.compile("impalad version ([0-9.]+)[- ].*");
      Matcher m = p.matcher(fullVersion());
      Preconditions.checkState(m.matches());
      version = m.group(1);
    }
    return version;
  }

  public QueryPlan plan() {
    if (plan == null) {
      plan = new QueryPlan(this);
    }
    return plan;
  }

  public void computePlanSummary() {
    plan().parseDetails();
    plan().parseSummary();
  }

  public void parsePlanDetails() {
    plan().parsePlanDetails();
  }

  public void expandExecNodes() {
    execNode.expand(this);
  }

  public ExecPNode exec() {
    expandExecNodes();
    return execNode;
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
