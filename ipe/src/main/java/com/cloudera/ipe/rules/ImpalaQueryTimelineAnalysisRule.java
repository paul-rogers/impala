// Copyright (c) 2016 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.joda.time.Duration;

/**
 * This class analyzes the query timeline from the runtime profile.
 */
public class ImpalaQueryTimelineAnalysisRule implements ImpalaAnalysisRule {

  public static final String ADMISSION_WAIT = "admission_wait";

  private static final String COMPLETED_ADMISSION = "Completed admission";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    Duration dur = tree.getEventWaitTime(
        COMPLETED_ADMISSION,
        IPEConstants.QUERY_TIMELINE_INDEX);
    if (dur == null) {
      return ImmutableMap.of();
    } else {
      return ImmutableMap.of(ADMISSION_WAIT, String.valueOf(dur.getMillis()));
    }
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(AttributeMetadata.newBuilder()
        .setName(ADMISSION_WAIT)
        .setDisplayNameKey("impala.analysis.admission_wait.name")
        .setDescriptionKey("impala.analysis.admission_wait.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of()).setSupportsHistograms(true)
        .build());
  }
}
