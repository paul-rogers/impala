// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TRuntimeProfileNode;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.cloudera.ipe.util.HistogramHelper;
import com.cloudera.ipe.util.HistogramHelper.CutPointsInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class ImpalaRuntimeProfileFragmentNode extends ImpalaRuntimeProfileNode {

  private static final int MAX_BINS = 20;
  private static List<ImpalaFragmentMetric> METRICS_FOR_CROSSFILTER =
      ImmutableList.of(ImpalaFragmentMetric.BYTES_READ,
                       ImpalaFragmentMetric.BYTES_STREAMED,
                       ImpalaFragmentMetric.TOTAL_TIME);
  
  public ImpalaRuntimeProfileFragmentNode(
      final TRuntimeProfileNode node,
      final ImpalaRuntimeProfileNode parent) {
    super(node, parent);
  }
  
  /**
   * Get the averaged fragment node. Can return null.
   * @return
   */
  public ImpalaRuntimeProfileNode getAveragedFragmentNode() {
    // This is a bit hacky, but there isn't really a great way to do this.
    // The average fragment should be at the same level of the tree as all
    // the other fragments, so let's check all this nodes parent's children
    if (this.thriftNode.getName() == null) {
      throw new ImpalaCorruptProfileException(
          "Corrupt runtime profile, node has no name");
    }
    ImpalaRuntimeProfileNode parent = this.parent;
    if (parent == null) {
      throw new ImpalaCorruptProfileException(
          "Fragment has no parent: " + this.thriftNode.getName());
    }
    String fragmentName = this.thriftNode.getName();
    for (ImpalaRuntimeProfileNode node : parent.getChildren()) {
      if (node.getThriftNode().getName().startsWith("Averaged " + fragmentName)) {
        return node;
      }
    }
    // Insert queries may have no averaged fragment node
    // See https://issues.cloudera.org/browse/IMPALA-348
    return null;
  }

  @Override
  public void setChildren(ImmutableList<ImpalaRuntimeProfileNode> nodes) {
    super.setChildren(nodes);
    // Make sure that all the children are instance nodes, like we expect
    for (ImpalaRuntimeProfileNode node : nodes) {
      if (!(node instanceof ImpalaRuntimeProfileInstanceNode)) {
        throw new ImpalaCorruptProfileException(
            "Fragment has non-instance children");
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  public List<ImpalaRuntimeProfileInstanceNode> getInstances() {
    // We know that this is a safe cast because we check in setChildren
    return (List<ImpalaRuntimeProfileInstanceNode>)(List<?>)children;
  }
  
  /**
   * Gets the maximum total time spent by any fragment instance. This gives a good
   * idea of how long this fragment took.
   * 
   * Returns null if no instances are found or if the all of the instances
   * don't have a TotalTime counter.
   */
  public Long getMaxTotalTime() {
    Long max = null;
    for (ImpalaRuntimeProfileNode child : children) {
      Long childTotalTime = child.getTotalTime();
      if (childTotalTime == null) {
        continue;
      }
      if (max == null || childTotalTime > max) {
        max = childTotalTime;
      }
    }
    return max;
  }
  
  /**
   * Returns all the metric information for the fragment. It contains a list
   * of metric and their metadata (type, label) and the metric values for each
   * instance.
   * @param translator - a function that translates the metric labels. Callers
   * should pass in Translator.TRANSLATE.
   * @return
   */
  @VisibleForTesting
  public ImpalaFragmentMetrics getMetrics(
      final Function<String, String> translator) {
    // Initialize the instance metrics map
    Map<String, Map<String, Double>> mapInstanceToMetricsMap = Maps.newHashMap();
    for (ImpalaRuntimeProfileInstanceNode instance : getInstances()) {
      mapInstanceToMetricsMap.put(instance.getInstanceGUID(), Maps.<String, Double>newHashMap());
    }

    List<ImpalaFragmentMetric> metrics = Lists.newArrayList();
    for (ImpalaFragmentMetric metric : METRICS_FOR_CROSSFILTER) {
      List<Double> allValuesForMetric = Lists.newArrayList();
      for (ImpalaRuntimeProfileInstanceNode instance : getInstances()) {
        Double value = instance.getMetric(metric);
        if (value != null) {
          allValuesForMetric.add(value);
          Map<String, Double> instanceMetricsMap = mapInstanceToMetricsMap.get(
              instance.getInstanceGUID());
          instanceMetricsMap.put(metric.getName(), value);
        }
      }
      // If we have any values for that metric, then add it to the result list.
      // Then check if it's missing from of the maps for each instance and in
      // that case add the default.
      if (allValuesForMetric.isEmpty()) {
        continue;
      }
      double sum = 0.0;
      for (Double value : allValuesForMetric) {
        sum += value;
      }
      CutPointsInfo cutPointsInfo = HistogramHelper.getSuggestedCutPoints(
          allValuesForMetric, MAX_BINS, metric.getCounterName());
      metrics.add(new ImpalaFragmentMetric(metric.getNodePrefix(),
          metric.getCounterName(),
          translator.apply(metric.getLabel()),
          metric.getType(),
          metric.getDefaultSelected(),
          metric.getDefaultValue(),
          sum / allValuesForMetric.size(),
          cutPointsInfo.getCutPoints(),
          cutPointsInfo.getBinScale(),
          cutPointsInfo.getScaleValue()));
      // Add in the default values if necessary
      for (Map<String, Double> instanceMetricsMap : mapInstanceToMetricsMap.values()) {
        if (!instanceMetricsMap.containsKey(metric.getName())) {
          instanceMetricsMap.put(metric.getName(), metric.getDefaultValue());
        }
      }
    }
    return new ImpalaFragmentMetrics(metrics, mapInstanceToMetricsMap);
  }
}
