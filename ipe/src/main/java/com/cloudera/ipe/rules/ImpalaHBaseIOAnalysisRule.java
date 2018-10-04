// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This analysis rule is responsible for extracting HBase IO statistics from
 * the profile.
 */
public class ImpalaHBaseIOAnalysisRule implements ImpalaAnalysisRule {

  public static final String HBASE_BYTES_READ = "hbase_bytes_read";
  public static final String HBASE_SCANNER_AVERAGE_BYTES_READ_PER_SECOND =
      "hbase_scanner_average_bytes_read_per_second";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Long totalBytesRead = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_BYTES_READ,
        IPEConstants.IMPALA_PROFILE_HBASE_SCAN_NODE_PREFIX);
    Long totalReadTime = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_RAW_HBASE_READ_TIME,
        IPEConstants.IMPALA_PROFILE_HBASE_SCAN_NODE_PREFIX);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (totalBytesRead != null) {
      b.put(HBASE_BYTES_READ,
            String.valueOf(totalBytesRead));

      if (totalReadTime != null) {
        double readBytesPerSecond;
        if (totalReadTime == 0) {
          readBytesPerSecond = 0L;
        } else {
          double readTimeInSeconds = (double) totalReadTime / 
              (double) IPEConstants.SECOND_IN_NANO_SECONDS;
          readBytesPerSecond = (double) totalBytesRead / (double) readTimeInSeconds;
        }
        b.put(HBASE_SCANNER_AVERAGE_BYTES_READ_PER_SECOND,
            String.valueOf(readBytesPerSecond));
      }
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata totalReadMetadata = AttributeMetadata.newBuilder()
        .setName(HBASE_BYTES_READ)
        .setDisplayNameKey("impala.analysis.hbase_bytes_read.name")
        .setDescriptionKey("impala.analysis.hbase_bytes_read.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata throughputMetadata = AttributeMetadata.newBuilder()
        .setName(HBASE_SCANNER_AVERAGE_BYTES_READ_PER_SECOND)
        .setDisplayNameKey("impala.analysis.hbase_scanner_average_bytes_read_per_second.name")
        .setDescriptionKey("impala.analysis.hbase_scanner_average_bytes_read_per_second.description")
        .setFilterType(AttributeDataType.BYTES_PER_SECOND)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(
        totalReadMetadata,
        throughputMetadata);
  }
}
