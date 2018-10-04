// Copyright (c) 2014 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This analysis rule extracts attributes related to the query plan.
 */
public class ImpalaQueryPlanAnalysisRule implements ImpalaAnalysisRule {

  public static final String STATS_MISSING = "stats_missing";
  public static final String STATS_CORRUPT = "stats_corrupt";
  public static final String ESTIMATED_PER_NODE_PEAK_MEMORY =
      "estimated_per_node_peak_memory";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();

    String missingStatsWarning =
        tree.getSummaryMap().get(IPEConstants.IMPALA_QUERY_ATTRIBUTE_TABLES_MISSING_STATS);
    b.put(STATS_MISSING, Boolean.toString(missingStatsWarning != null));

    String corruptStatsWarning =
            tree.getSummaryMap().get(IPEConstants.IMPALA_QUERY_ATTRIBUTE_TABLES_CORRUPT_STATS);
    b.put(STATS_CORRUPT, Boolean.toString(corruptStatsWarning != null));

    String memoryEstimate =
        tree.getSummaryMap().get(IPEConstants.IMPALA_QUERY_ATTRIBUTE_ESTIMATED_PER_HOST_MEMORY);
    if (memoryEstimate != null) {
      b.put(ESTIMATED_PER_NODE_PEAK_MEMORY, memoryEstimate);
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata statsMissingWarningMetadata = AttributeMetadata.newBuilder()
        .setName(STATS_MISSING)
        .setDisplayNameKey("impala.analysis.stats_missing.name")
        .setDescriptionKey("impala.analysis.stats_missing.description")
        .setFilterType(AttributeDataType.BOOLEAN)
        .setValidValues(ImmutableList.<String>of("true", "false"))
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata statsCorruptWarningMetadata = AttributeMetadata.newBuilder()
        .setName(STATS_CORRUPT)
        .setDisplayNameKey("impala.analysis.stats_corrupt.name")
        .setDescriptionKey("impala.analysis.stats_corrupt.description")
        .setFilterType(AttributeDataType.BOOLEAN)
        .setValidValues(ImmutableList.<String>of("true", "false"))
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata estimatedPerNodePeakMemoryMetadata = AttributeMetadata.newBuilder()
        .setName(ESTIMATED_PER_NODE_PEAK_MEMORY)
        .setDisplayNameKey("impala.analysis.estimated_per_node_peak_memory.name")
        .setDescriptionKey("impala.analysis.estimated_per_node_peak_memory.description")
        .setFilterType(AttributeDataType.BYTES)
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(statsMissingWarningMetadata,
                            statsCorruptWarningMetadata,
                            estimatedPerNodePeakMemoryMetadata);
  }
}
