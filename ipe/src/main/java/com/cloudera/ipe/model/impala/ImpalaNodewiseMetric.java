// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This class is used to store the plan node level metrics across all the
 * instances in the given impala profile. An example of a plan node is
 * "HDFS_SCAN_NODE (id=1)" which is different from "HDFS_SCAN_NODE (id=0)". The
 * list of metrics across all instances are stored as a key, <list> in the map.
 */
public class ImpalaNodewiseMetric {
  private final String nodeName;
  private final Map<String, List<Double>> metricValues = Maps.newHashMap();;

  public ImpalaNodewiseMetric(String nodeName) {
    Preconditions.checkNotNull(nodeName);
    this.nodeName = nodeName;
  }

  /**
   * Appends a new metric value by the given name, to the list.
   * Examples: PeakMemoryUsage, BytesRead, DelimiterParseTime.
   */
  public void addMetric(String metricName, double value) {
    Preconditions.checkNotNull(metricName);
    if (!metricValues.containsKey(metricName)) {
      metricValues.put(metricName, Lists.<Double>newArrayList());
    }
    metricValues.get(metricName).add(value);
  }

  /**
   * Returns the name of the plan node.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Returns the list of metrics present in the plan node.
   */
  public Collection<String> getAllMetrics() {
    return metricValues.keySet();
  }

  /**
   * Returns all the values of a given metric. These metrics come from different
   * instance nodes within the fragment/coordinator node.
   */
  public List<Double> getAllMetricValues(String metricName) {
    Preconditions.checkNotNull(metricName);
    return metricValues.get(metricName);
  }

  /**
   * Computes the minimum value among the metric values of all instances.
   */
  public Double getMinValue(String metricName) {
    Preconditions.checkNotNull(metricName);
    List<Double> arr = getAllMetricValues(metricName);
    if (arr == null || arr.isEmpty()) {
      return null;
    }
    return Doubles.min(Doubles.toArray(arr));
  }

  /**
   * Computes the maximum value among the metric values of all instances.
   */
  public Double getMaxValue(String metricName) {
    Preconditions.checkNotNull(metricName);
    List<Double> arr = getAllMetricValues(metricName);
    if (arr == null || arr.isEmpty()) {
      return null;
    }

    return Doubles.max(Doubles.toArray(arr));
  }

  /**
   * Computes the sum of the metric values of all instances.
   */
  public Double getSumValue(String metricName) {
    Preconditions.checkNotNull(metricName);
    List<Double> arr = getAllMetricValues(metricName);
    if (arr == null || arr.isEmpty()) {
      return null;
    }
    double total = 0;
    for (Double val : arr) {
      total += val;
    }
    return total;
  }

  /**
   * Computes the minimum value among the metric values of all instances.
   */
  public Double getAvgValue(String metricName) {
    Preconditions.checkNotNull(metricName);
    List<Double> arr = getAllMetricValues(metricName);
    if (arr == null || arr.isEmpty()) {
      return null;
    }
    double total = 0;
    for (Double val : arr) {
      total += val;
    }
    return total / arr.size();
  }

  /**
   * Returns the list of metrics map that contains the list of values collected.
   */
  public Map<String, List<Double>> getMetricValues() {
    return metricValues;
  }

  /**
   * Computes the standard deviation value among the metric values of all instances.
   */
  public Double getStdValue(List<Double> arr) {
    Preconditions.checkNotNull(arr);

    StandardDeviation sd = new StandardDeviation(false);

    double[] sd_array = new double[arr.size()];
    for (int i = 0; i < sd_array.length; i++) {
      sd_array[i] = arr.get(i);
    }

    return sd.evaluate(sd_array);
  }

}
