package com.cloudera.cmf;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;

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
    protected final int childCount;

    public ProfileNode(ProfileAnalyzer analyzer, int index) {
      this.analyzer = analyzer;
      this.index = index;
      node = analyzer.node(index);
      childCount = node.getNum_children();
      firstChild = index + 1;
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

  public static class HelperNode extends ProfileNode {

    public HelperNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
    }
  }

  public static class ExecProfileNode extends ProfileNode {

    public ExecProfileNode(ProfileAnalyzer analyzer, int index) {
      super(analyzer, index);
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
  public static class QueryNode extends ProfileNode {

    public static String IMPALA_SERVER_NODE = "ImpalaServer";
    public static String EXEC_PROFILE_NODE = "Execution Profile";
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

    public enum SummaryState {
      // TODO: Fill in other. Failure? Cancelled?

      OK, FAILED, OTHER
    }

    private final HelperNode summaryNode;
    private HelperNode serverNode;
    private ExecProfileNode execNode;
    private String queryId;

    public QueryNode(ProfileAnalyzer analyzer) {
      super(analyzer, 0);
      summaryNode = new HelperNode(analyzer, 1);
      for (int i = 1; i < childCount; i++) {
        TRuntimeProfileNode child = analyzer.node(firstChild + i);
        String childName = child.getName();
        if (childName.equals(IMPALA_SERVER_NODE)) {
          serverNode = new HelperNode(analyzer, i);
        } else if (childName.startsWith(EXEC_PROFILE_NODE)) {
          execNode = new ExecProfileNode(analyzer, i);
        } else {
          throw new IllegalStateException("Unknown node type: " + childName);
        }
      }
//      for (int i = 0; i < childCount; i++) {
//        TRuntimeProfileNode child = analyzer.node(firstChild + i);
//        System.out.println(child.getName());
//      }
      Pattern p = Pattern.compile("\\(id=([^)]+)\\)");
      Matcher m = p.matcher(name());
      if (m.find()) {
        queryId = m.group(1);
      }
    }

    public String queryId() { return queryId; }

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
      return summaryNode.attrib(attrib.key());
    }

    public QueryType type() {
      try {
        return QueryType.typeFor(attrib(Attrib.QUERY_TYPE));
      } catch (Exception e) {
        return null;
      }
    }

    public void generateAttribs() {
      summaryNode.generateAttribs();
    }
  }

  private final String queryId;
  private final String label;
  private final TRuntimeProfileTree profile;
  private final QueryNode root;

  public ProfileAnalyzer(TRuntimeProfileTree profile) {
    this(profile, null, null);
  }

  public ProfileAnalyzer(TRuntimeProfileTree profile,
      String queryId, String label) {
    this.queryId = queryId;
    this.label = label;
    this.profile = profile;
    root = new QueryNode(this);
  }

  public TRuntimeProfileNode node(int i) {
    return profile.nodes.get(i);
  }

  public String queryId() { return queryId; }
  public String label() { return label; }

  public String title() {
    StringBuilder buf = new StringBuilder();
    if (label != null) {
      buf
        .append(label)
        .append(": ");
    }
    buf
      .append(root.type().name())
      .append(" (")
      .append(root.summaryState().name())
      .append(")");
    String id = queryId == null ? root.queryId() : queryId;
    if (id != null) {
      buf
        .append(" - ID = ")
        .append(id);
    }
    return buf.toString();
  }

  public QueryNode query() { return root; }
}
