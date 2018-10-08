package com.cloudera.cmf.analyzer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;

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
public class ProfileFacade {

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

  public static class RootPNode extends ProfileNode {

    public static String IMPALA_SERVER_NODE = "ImpalaServer";
    public static String EXEC_PROFILE_NODE = "Execution Profile";

    private final SummaryPNode summaryNode;
    private ProfileNode.HelperPNode serverNode;
    private ExecPNode execNode;
    public String queryId;

    public RootPNode(ProfileFacade analyzer) {
      super(analyzer, 0);
      Preconditions.checkState(childCount() == 1 || childCount() == 3);
      summaryNode = new SummaryPNode(analyzer, 1);
      if (childCount() == 3) {
        serverNode = new ProfileNode.HelperPNode(analyzer, 2);
        execNode = new ExecPNode(analyzer, 3);
      }
      Pattern p = Pattern.compile("\\(id=([^)]+)\\)");
      Matcher m = p.matcher(name());
      if (m.find()) {
        queryId = m.group(1);
      }
    }

    public SummaryPNode summary() { return summaryNode; }
    public ProfileNode.HelperPNode serverNode() { return serverNode; }
    public ExecPNode execNode() { return execNode; }
    public String queryId() { return queryId; }
  }

  private final String queryId;
  private final String label;
  private final TRuntimeProfileTree profile;
  private final RootPNode root;
  private final SummaryPNode summary;
  private QueryPlan plan;
  private QueryDAG dag;


  public ProfileFacade(TRuntimeProfileTree profile) {
    this(profile, null, null);
  }

  public ProfileFacade(TRuntimeProfileTree profile,
      String queryId, String label) {
    this.queryId = queryId;
    this.label = label;
    this.profile = profile;
    root = new RootPNode(this);
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

  public SummaryPNode summary() { return summary; }

  public String stmt() {
    return summary.attrib(SummaryPNode.Attrib.SQL_STATEMENT);
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

  public ExecPNode exec() {
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
