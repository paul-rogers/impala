// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TUnit;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.util.HistogramHelper.BinScale;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * The metadata for the metric. This includes two types of information:
 * 1. The static metric metadata like the name and counter type
 * 2. The dynamic metric metadata based on the instance data, like average
 * value and suggested histogram cut-points.
 */
public class ImpalaFragmentMetric {

  public static final ImpalaFragmentMetric BYTES_READ = new ImpalaFragmentMetric(
      IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX,
      IPEConstants.IMPALA_PROFILE_BYTES_READ,
      "label.impala.fragment.metrics.bytes_read",
      TUnit.BYTES,
      true,
      0L,
      0.0,
      ImmutableList.<Double>of(),
      BinScale.LINEAR,
      1.0);
  public static final ImpalaFragmentMetric BYTES_STREAMED = new ImpalaFragmentMetric(
      IPEConstants.IMPALA_PROFILE_DATA_STREAM_SENDER_NODE,
      IPEConstants.IMPALA_PROFILE_BYTES_SENT,
      "label.impala.fragment.metrics.bytes_sent",
      TUnit.BYTES,
      false,
      0L,
      0.0,
      ImmutableList.<Double>of(),
      BinScale.LINEAR,
      1.0);
  public static final ImpalaFragmentMetric TOTAL_TIME = new ImpalaFragmentMetric(
      IPEConstants.IMPALA_PROFILE_INSTANCE_PREFIX,
      IPEConstants.IMPALA_PROFILE_TOTAL_TIME,
      "label.impala.fragment.metrics.total_time",
      TUnit.TIME_NS,
      true,
      0L,
      0.0,
      ImmutableList.<Double>of(),
      BinScale.LINEAR,
      1.0);

  private final String nodePrefix;
  private final String counterName;
  private final String label;
  private final TUnit type;
  // Indicates whether the metric should be shown by default in the crossfilter
  private final boolean defaultSelected;
  private final double defaultValue;
  private final double averageValue;
  // This is a list of values to divide bins at. For example if this list is
  // {2, 10, 20} then we're suggesting we use 4 bins:
  // [negative infinity, 2), [2, 10), [10, 20), and [20, infinity)
  private final List<Double> suggestedCutPoints;
  private final BinScale binScale;
  private final double scaleValue;

  public ImpalaFragmentMetric(
      String nodePrefix,
      String counterName,
      String label,
      TUnit type,
      boolean defaultSelected,
      double defaultValue,
      double averageValue,
      List<Double> suggestedCutPoints,
      BinScale binScale,
      double scaleValue) {
    Preconditions.checkNotNull(nodePrefix);
    Preconditions.checkNotNull(counterName);
    Preconditions.checkNotNull(label);
    Preconditions.checkNotNull(type);
    this.nodePrefix = nodePrefix;
    this.counterName = counterName;
    this.label = label;
    this.type = type;
    this.defaultSelected = defaultSelected;
    this.defaultValue = defaultValue;
    this.averageValue = averageValue;
    this.suggestedCutPoints = suggestedCutPoints;
    this.binScale = binScale;
    this.scaleValue = scaleValue;
  }

  /**
   * Returns the name of the metric. This name is specified as
   * nodeNamePrefix.counterName, for example HDFS_SCAN_NODE.BytesRead
   * @return
   */
  public String getName() {
    return nodePrefix + "." + counterName;
  }

  public String getNodePrefix() {
    return nodePrefix;
  }

  public String getCounterName() {
    return counterName;
  }

  public String getLabel() {
    return label;
  }

  public TUnit getType() {
    return type;
  }

  public boolean getDefaultSelected() {
    return defaultSelected;
  }

  public double getDefaultValue() {
    return defaultValue;
  }

  public double getAverageValue() {
    return averageValue;
  }

  public List<Double> getSuggestedCutPoints() {
    return suggestedCutPoints;
  }

  public BinScale getBinScale() {
    return binScale;
  }

  public double getScaleValue() {
    return scaleValue;
  }
}
