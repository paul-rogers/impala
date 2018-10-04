// Copyright (c) 2016 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This class extracts the admission result from the runtime profile.
 */
public class ImpalaAdmissionResultAnalysisRule implements ImpalaAnalysisRule {

  public static final String ADMISSION_RESULT = "admission_result";
  public static final String REJECTED = "Rejected";
  public static final String TIMED_OUT = "Timed out (queued)";

  private static final String ADMISSION_RESULT_INFO_STRING = "Admission result";

  @VisibleForTesting
  private static final String UNKNOWN = "Unknown";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    String status = tree.getSummaryMap().get(ADMISSION_RESULT_INFO_STRING);
    if (status == null) {
      status = UNKNOWN;
    }
    return ImmutableMap.of(ADMISSION_RESULT, status);
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(AttributeMetadata.newBuilder()
        .setName(ADMISSION_RESULT)
        .setDisplayNameKey("impala.analysis.admission_result.name")
        .setDescriptionKey("impala.analysis.admission_result.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build());
  }
}
