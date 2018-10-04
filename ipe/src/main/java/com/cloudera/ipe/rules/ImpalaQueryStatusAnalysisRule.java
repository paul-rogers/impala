// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This class extracts the query status from the runtime profile. The query
 * status can be used to determine the error that caused the query to fail.
 */
public class ImpalaQueryStatusAnalysisRule implements ImpalaAnalysisRule {

  public static final String OOM = "oom";

  private static final String MEMORY_LIMIT_EXCEEDED =
      "Memory limit exceeded".toLowerCase();
  private static final String MEMORY_RESERVE_FAIL =
      "Failed to get minimum memory reservation".toLowerCase();
  private final ImmutableList<String> oomIndicatorStrings = ImmutableList
      .of(MEMORY_LIMIT_EXCEEDED, MEMORY_RESERVE_FAIL);

  @VisibleForTesting
  private static final String UNKNOWN = "Unknown";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    String queryStatus = Preconditions.checkNotNull(getQueryStatus(tree));
    builder.put(IPEConstants.IMPALA_QUERY_ATTRIBUTE_QUERY_STATUS, queryStatus);

    String queryState = tree.getSummaryMap().get(
        IPEConstants.IMPALA_QUERY_STATE_INFO_STRING);
    boolean oom = isOOM(queryStatus, queryState);
    builder.put(OOM, String.valueOf(oom));
    return builder.build();
  }

  /**
   * Determine if the query is failed with OOM error.
   */
  private boolean isOOM(String queryStatus, String queryState) {
    if (!IPEConstants.IMPALA_QUERY_STATE_EXCEPTION.equals(queryState)) {
      return false;
    }
    for (String status : oomIndicatorStrings) {
      if (queryStatus.toLowerCase().contains(status)) {
        return true;
      }
    }
    return false;
  }

  private String getQueryStatus(ImpalaRuntimeProfileTree tree) {
    // First look in the summary map. If it's not there fall back to the
    // execution node
    String status = tree.getSummaryMap().get(
        IPEConstants.IMPALA_QUERY_STATUS_INFO_STRING);
    if (status != null) {
      return status;
    }
    ImpalaRuntimeProfileNode executionNode = tree.getExecutionProfileNode();
    if (executionNode == null) {
      return UNKNOWN;
    }
    status = executionNode.getThriftNode().getInfo_strings()
        .get(IPEConstants.IMPALA_QUERY_STATUS_INFO_STRING);
    if (status == null) {
      status = UNKNOWN;
    }
    return status;
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(
        AttributeMetadata
            .newBuilder()
            .setName(IPEConstants.IMPALA_QUERY_ATTRIBUTE_QUERY_STATUS)
            .setDisplayNameKey("impala.analysis.query_status.name")
            .setDescriptionKey("impala.analysis.query_status.description")
            .setFilterType(AttributeDataType.STRING)
            // We don't know all the possible query statuses, so let's leave
            // this
            // empty
            .setValidValues(ImmutableList.<String>of())
            .setSupportsHistograms(true)
            .build(),
        AttributeMetadata
            .newBuilder()
            .setName(OOM)
            .setDisplayNameKey("impala.analysis.oom.name")
            .setDescriptionKey("impala.analysis.oom.description")
            .setFilterType(AttributeDataType.BOOLEAN)
            .setValidValues(ImmutableList.<String>of("true", "false"))
            .setSupportsHistograms(true)
            .build());
  }
}
