// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.TimedDataPoint;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.TimeseriesDataUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This analysis rule is responsible for extracting memory statistics from
 * the profile.
 */
public class ImpalaMemoryUsageAnalysisRule implements ImpalaAnalysisRule {
  private static Logger LOG = LoggerFactory.getLogger(
      ImpalaMemoryUsageAnalysisRule.class);

  public static final String MEMORY_PER_NODE_PEAK = "memory_per_node_peak";
  public static final String MEMORY_PER_NODE_PEAK_NODE = "memory_per_node_peak_node";
  public static final String MEMORY_AGGREGATE_PEAK = "memory_aggregate_peak";
  public static final String MEMORY_ACCRUAL = "memory_accrual";

  // We're parsing something that looks like the following:
  //
  // d2401.halxg.cloudera.com:22000(88.07 MB) d2412.halxg.cloudera.com:22000(0)
  private static Pattern pattern = Pattern.compile("(\\S+)\\((\\S+)[ ]?([a-zA-Z]+)?\\)");

  private ImmutableList<DateTimeFormatter> profileTimeFormatters = null;

  public ImpalaMemoryUsageAnalysisRule(ImmutableList<DateTimeFormatter> profileTimeFormatters) {
    this.profileTimeFormatters = profileTimeFormatters;
  }

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();

    ImpalaRuntimeProfileNode executionNode = tree.getExecutionProfileNode();
    if (executionNode == null) {
      return b.build();
    }

    String usage = executionNode.getThriftNode().getInfo_strings()
        .get(IPEConstants.IMPALA_PER_NODE_PEAK_MEMORY_USAGE);
    if (usage == null) {
      return b.build();
    }

    PerNodePeak perNodePeak = null;
    try {
      perNodePeak = parseUsage(usage);
    } catch (Exception e) {
      LOG.warn("Failed to parse per-node peak memory string: {}",
          usage);
    }

    if (perNodePeak != null) {
      b.put(MEMORY_PER_NODE_PEAK, Double.toString(perNodePeak.peak));
      b.put(MEMORY_PER_NODE_PEAK_NODE, perNodePeak.node);
    } else {
      LOG.warn("Failed to match per-node peak memory string: {}",
          usage);
    }

    Instant startTime = tree.getStartTime(profileTimeFormatters);
    if (startTime != null) {
      Instant endTime = tree.getEndTime(profileTimeFormatters);
      if (endTime == null) {
        endTime = new Instant();
      }
      List<TimedDataPoint> memoryStream =  TimeseriesDataUtil.getTimeSeriesData(
          tree,
          IPEConstants.IMPALA_TIME_SERIES_COUNTER_MEMORY_USAGE,
          startTime,
          endTime);
      double total = 0.0;
      Double aggregatePeak = null;
      Instant lastPointTime = startTime;
      for (TimedDataPoint p : memoryStream) {
        long ms = p.getTimestamp().getMillis() - lastPointTime.getMillis();
        Preconditions.checkState(ms >= 0);
        if (aggregatePeak == null || p.getValue() > aggregatePeak) {
          aggregatePeak = p.getValue();
        }
        total += (ms / 1000.0) * p.getValue();
        lastPointTime = p.getTimestamp();
      }
      if (aggregatePeak != null) {
        if (perNodePeak != null && perNodePeak.peak > aggregatePeak) {
          aggregatePeak = perNodePeak.peak;
        }
        b.put(MEMORY_AGGREGATE_PEAK, aggregatePeak.toString());
        b.put(MEMORY_ACCRUAL, Double.toString(total));
      }
    }

    return b.build();
  }

  @VisibleForTesting
  public static class PerNodePeak {
    public final String node;
    public final double peak;

    PerNodePeak(String node, Double peak) {
      Preconditions.checkNotNull(node);
      Preconditions.checkNotNull(peak);
      this.node = node;
      this.peak = peak;
    }
  }

  @VisibleForTesting
  public PerNodePeak parseUsage(String usage) {
    Preconditions.checkNotNull(usage);

    String node = null;
    Double perNodePeak = null;

    Matcher m = pattern.matcher(usage);
    while (m.find()) {
      double v = Double.parseDouble(m.group(2));
      String unit = m.group(3);
      // These are matched with Impala's GetByteUnit function.
      if (unit == null) {
        v *= 1;// bytes
      } else if (unit.equals("KB")) {
        v *= IPEConstants.ONE_KILOBYTE;
      } else if (unit.equals("MB")) {
        v *= IPEConstants.ONE_MEGABYTE;
      } else if (unit.equals("GB")) {
        v *= IPEConstants.ONE_GIGABYTE;
      }
      if (perNodePeak == null || v > perNodePeak) {
        perNodePeak = v;
        node = m.group(1);
      }
    }

    return new PerNodePeak(node, perNodePeak);
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata memoryPerNodePeakMetadata = AttributeMetadata.newBuilder()
        .setName(MEMORY_PER_NODE_PEAK)
        .setDisplayNameKey("impala.analysis.memory_per_node_peak.name")
        .setDescriptionKey("impala.analysis.memory_per_node_peak.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata memoryPerNodePeakNodeMetadata = AttributeMetadata.newBuilder()
        .setName(MEMORY_PER_NODE_PEAK_NODE)
        .setDisplayNameKey("impala.analysis.memory_per_node_peak_node.name")
        .setDescriptionKey("impala.analysis.memory_per_node_peak_node.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata memoryAggregatePeakMetadata = AttributeMetadata.newBuilder()
        .setName(MEMORY_AGGREGATE_PEAK)
        .setDisplayNameKey("impala.analysis.memory_aggregate_peak.name")
        .setDescriptionKey("impala.analysis.memory_aggregate_peak.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata memoryAccrualMetadata = AttributeMetadata.newBuilder()
        .setName(MEMORY_ACCRUAL)
        .setDisplayNameKey("impala.analysis.memory_accrual.name")
        .setDescriptionKey("impala.analysis.memory_accrual.description")
        .setFilterType(AttributeDataType.BYTE_SECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
   return ImmutableList.of(
        memoryPerNodePeakMetadata,
        memoryPerNodePeakNodeMetadata,
        memoryAggregatePeakMetadata,
        memoryAccrualMetadata);
  }
}
