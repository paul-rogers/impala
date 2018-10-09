package com.cloudera.cmf.profile;

import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.impala.thrift.TEventSequence;

import com.cloudera.cmf.profile.ProfileFacade.QueryType;
import com.cloudera.cmf.profile.ProfileFacade.SummaryState;

/**
 * Represents the statement as a whole. Has three children:
 * <ol>
 * <li>Summary (req)</li>
 * <li>ImpalaServer (opt)</li>
 * <li>Execution Profile (opt)</li>
 * </ol>
 */
public class SummaryPNode extends ProfileNode {

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

  private long startTimestamp;
  private long endTimestamp;

  public SummaryPNode(ProfileFacade analyzer, int index) {
    super(analyzer, index);
  }

  @Override
  public PNodeType nodeType() { return PNodeType.SUMMARY; }

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

  public long durationMs() {
    return endTimestamp() - startTimestamp();
  }

  public long startTimestamp() {
    if (startTimestamp == 0) {
      startTimestamp = ParseUtils.parseStartEndTimestamp(
          attrib(Attrib.START_TIME));
    }
    return startTimestamp;
  }

  public long endTimestamp() {
    if (endTimestamp == 0) {
      endTimestamp = ParseUtils.parseStartEndTimestamp(
          attrib(Attrib.END_TIME));
    }
    return endTimestamp;
  }

  public String attrib(SummaryPNode.Attrib attrib) {
    return attrib(attrib.key());
  }

  public TEventSequence events(SummaryPNode.EventSequence attrib) {
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