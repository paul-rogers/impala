// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This analysis rule is responsible for extracting information on the total
 * bytes streamed.
 */
public class ImpalaDataStreamerAnalysisRule implements ImpalaAnalysisRule {

  public static final String BYTES_STREAMED = "bytes_streamed";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Long totalBytesStreamed = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_BYTES_SENT,
        IPEConstants.IMPALA_PROFILE_DATA_STREAM_SENDER_NODE);
    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (totalBytesStreamed != null) {
        b.put(BYTES_STREAMED, String.valueOf(totalBytesStreamed));
    }
    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata totalBytesSentMetadata =
        AttributeMetadata.newBuilder()
          .setName(BYTES_STREAMED)
          // Alias for name in 4.x
          .setAliases(ImmutableList.of("totalBytesStreamed"))
          .setDisplayNameKey("impala.analysis.bytes_streamed.name")
          .setDescriptionKey("impala.analysis.bytes_streamed.description")
          .setFilterType(AttributeDataType.BYTES)
          .setValidValues(ImmutableList.<String>of())
          .setSupportsHistograms(true)
          .build();
    return ImmutableList.of(totalBytesSentMetadata);
  }
}
