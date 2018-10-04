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

public class ImpalaResourceMetricsConverterAnalysisRule
    implements ImpalaAnalysisRule {

  public static final String WORK_CPU_TIME =
      "cm_cpu_milliseconds";

  public static final AttributeMetadata CPU_TIME_METADATA =
      AttributeMetadata.newBuilder()
        .setName(WORK_CPU_TIME)
        .setDisplayNameKey("cm.analysis.work_cpu_time.name")
        .setDescriptionKey("cm.analysis.work_cpu_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    Long cpuTimeNanos = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_TOTAL_CPU_TIME,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (null != cpuTimeNanos) {
      b.put(WORK_CPU_TIME,
            String.valueOf((double) cpuTimeNanos / IPEConstants.NANOS_PER_MILLIS));
    }
    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(
        CPU_TIME_METADATA);
  }
}
