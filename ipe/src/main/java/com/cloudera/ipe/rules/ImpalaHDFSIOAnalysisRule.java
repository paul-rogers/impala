// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.IPEUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This analysis rule is responsible for extracting HDFS IO statistics from
 * the profile.
 */
public class ImpalaHDFSIOAnalysisRule implements ImpalaAnalysisRule {

  public static final String HDFS_BYTES_READ =
      "hdfs_bytes_read";
  public static final String HDFS_BYTES_READ_LOCAL =
      "hdfs_bytes_read_local";
  public static final String HDFS_BYTES_READ_LOCAL_PERCENTAGE =
      "hdfs_bytes_read_local_percentage";
  public static final String HDFS_BYTES_READ_REMOTE =
      "hdfs_bytes_read_remote";
  public static final String HDFS_BYTES_READ_REMOTE_PERCENTAGE =
      "hdfs_bytes_read_remote_percentage";
  public static final String HDFS_BYTES_READ_SHORT_CIRCUIT =
      "hdfs_bytes_read_short_circuit";
  public static final String HDFS_BYTES_READ_SHORT_CIRCUIT_PERCENTAGE =
      "hdfs_bytes_read_short_circuit_percentage";
  public static final String HDFS_BYTES_READ_FROM_CACHE =
      "hdfs_bytes_read_from_cache";
  public static final String HDFS_BYTES_READ_FROM_CACHE_PERCENTAGE =
      "hdfs_bytes_read_from_cache_percentage";
  public static final String HDFS_SCANNER_AVERAGE_BYTES_READ_PER_SECOND =
      "hdfs_scanner_average_bytes_read_per_second";
  public static final String HDFS_BYTES_SKIPPED =
      "hdfs_bytes_skipped";
  public static final String HDFS_AVERAGE_SCAN_RANGE =
      "hdfs_average_scan_range";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Long totalBytesRead = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_BYTES_READ,
        IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
    Long totalReadTime = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_RAW_HDFS_READ_TIME,
        IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
    Long totalBytesSkipped = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_BYTES_SKIPPED,
        IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
    Double averageScanRange = tree.getAverageScanRange();

    // See OPSAPS-26157: the following statistics are updated separately from
    // those above and may give misleading values for executing queries. In
    // particular it's possible to infer that many bytes read are not local or
    // not cached or not going to the short circuit read path if these values
    // are much smaller than the bytes read value. To avoid this, we simply do
    // not collect these for executing queries.
    Long totalBytesReadLocal = null;
    Long totalBytesReadShortCircuit = null;
    Long totalBytesReadFromCache = null;
    if (tree.hasEndTime()) {
      totalBytesReadLocal = tree.getSumAllCounterValues(
          IPEConstants.IMPALA_PROFILE_BYTES_READ_LOCAL,
          IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
      totalBytesReadShortCircuit = tree.getSumAllCounterValues(
          IPEConstants.IMPALA_PROFILE_BYTES_READ_SHORT_CIRCUIT,
          IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
      totalBytesReadFromCache = tree.getSumAllCounterValues(
          IPEConstants.IMPALA_PROFILE_BYTES_READ_FROM_CACHE,
          IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
    }

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (totalBytesRead != null) {
      b.put(HDFS_BYTES_READ,
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
        b.put(HDFS_SCANNER_AVERAGE_BYTES_READ_PER_SECOND,
            String.valueOf(readBytesPerSecond));
      }
    }

    if (totalBytesReadLocal != null) {
      b.put(HDFS_BYTES_READ_LOCAL,
            String.valueOf(totalBytesReadLocal));
      Long localPercentage =
          IPEUtils.computePercentage(totalBytesReadLocal, totalBytesRead);
      if (localPercentage != null) {
        b.put(HDFS_BYTES_READ_LOCAL_PERCENTAGE,
            String.valueOf(localPercentage));
      }
      if (totalBytesRead != null) {
        long totalBytesReadRemote = totalBytesRead - totalBytesReadLocal;
        b.put(HDFS_BYTES_READ_REMOTE, String.valueOf(totalBytesReadRemote));
        Long remotePercentage =
            IPEUtils.computePercentage(totalBytesReadRemote, totalBytesRead);
        if (remotePercentage != null) {
          b.put(HDFS_BYTES_READ_REMOTE_PERCENTAGE,
              String.valueOf(remotePercentage));
        }
      }
    }

    if (totalBytesReadShortCircuit != null) {
      b.put(HDFS_BYTES_READ_SHORT_CIRCUIT,
            String.valueOf(totalBytesReadShortCircuit));
      Long shortCircuitPercentage =
          IPEUtils.computePercentage(totalBytesReadShortCircuit,
                                          totalBytesRead);
      if (shortCircuitPercentage != null) {
        b.put(HDFS_BYTES_READ_SHORT_CIRCUIT_PERCENTAGE,
            String.valueOf(shortCircuitPercentage));
      }
    }

    if (totalBytesReadFromCache != null) {
      b.put(HDFS_BYTES_READ_FROM_CACHE,
            String.valueOf(totalBytesReadFromCache));
      Long fromCachePercentage =
          IPEUtils.computePercentage(totalBytesReadFromCache,
                                          totalBytesRead);
      if (fromCachePercentage != null) {
        b.put(HDFS_BYTES_READ_FROM_CACHE_PERCENTAGE,
            String.valueOf(fromCachePercentage));
      }
    }

    if (totalBytesSkipped != null) {
      b.put(HDFS_BYTES_SKIPPED, String.valueOf(totalBytesSkipped));
    }

    if (averageScanRange != null) {
      b.put(HDFS_AVERAGE_SCAN_RANGE, String.valueOf(averageScanRange));
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata totalReadMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesRead"))
        .build();
    AttributeMetadata throughputMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_SCANNER_AVERAGE_BYTES_READ_PER_SECOND)
        .setDisplayNameKey("impala.analysis.hdfs_scanner_average_bytes_read_per_second.name")
        .setDescriptionKey("impala.analysis.hdfs_scanner_average_bytes_read_per_second.description")
        .setFilterType(AttributeDataType.BYTES_PER_SECOND)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadPerSecond"))
        .build();
    AttributeMetadata bytesSkippedMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_SKIPPED)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_skipped.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_skipped.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesSkipped"))
        .build();
    AttributeMetadata totalReadLocalMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_LOCAL)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_local.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_local.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadLocal"))
        .build();
    AttributeMetadata totalReadLocalPercentageMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_LOCAL_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_local_percentage.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_local_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadLocalPercentage"))
        .build();
    AttributeMetadata totalReadRemoteMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_REMOTE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_remote.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_remote.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadRemote"))
        .build();
    AttributeMetadata totalReadRemotePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_REMOTE_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_remote_percentage.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_remote_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadRemotePercentage"))
        .build();
    AttributeMetadata totalReadShortCircuitMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_SHORT_CIRCUIT)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_short_circuit.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_short_circuit.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadShortCircuit"))
        .build();
    AttributeMetadata totalReadShortCircuitPercentageMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_SHORT_CIRCUIT_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_short_circuit_percentage.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_short_circuit_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        // Alias for name in 4.x
        .setAliases(ImmutableList.of("totalHDFSBytesReadShortCircuitPercentage"))
        .build();
    AttributeMetadata totalReadFromCacheMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_FROM_CACHE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_from_cache.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_from_cache.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata totalReadFromCachePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_READ_FROM_CACHE_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_read_from_cache_percentage.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_read_from_cache_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata averageScanRangeMetadata = AttributeMetadata.newBuilder()
        .setName(HDFS_AVERAGE_SCAN_RANGE)
        .setDisplayNameKey("impala.analysis.hdfs_average_scan_range.name")
        .setDescriptionKey("impala.analysis.hdfs_average_scan_range.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(
        totalReadMetadata,
        throughputMetadata,
        bytesSkippedMetadata,
        totalReadLocalMetadata,
        totalReadLocalPercentageMetadata,
        totalReadRemoteMetadata,
        totalReadRemotePercentageMetadata,
        totalReadShortCircuitMetadata,
        totalReadShortCircuitPercentageMetadata,
        totalReadFromCacheMetadata,
        totalReadFromCachePercentageMetadata,
        averageScanRangeMetadata);
  }
}
