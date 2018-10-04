// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The metric information for an Impala fragment
 */
public class ImpalaFragmentMetrics {

  /**
   * The metric information for an instance.
   */
  public static class ImpalaInstanceMetrics {
    private final String instanceId;
    private final Map<String, Double> metricsToValues;
    
    public ImpalaInstanceMetrics(
        String instanceId,
        Map<String, Double> metricsToValues) {
      Preconditions.checkNotNull(instanceId);
      Preconditions.checkNotNull(metricsToValues);
      this.instanceId = instanceId;
      this.metricsToValues = metricsToValues;
    }

    public String getInstanceId() {
      return instanceId;
    }
    
    /**
     * Returns a map from metrics to values.
     */
    public Map<String, Double> getMetricsToValues() {
      return metricsToValues;
    }
  }
  
  private final List<ImpalaFragmentMetric> metricsMetadata;
  private final List<ImpalaInstanceMetrics> instanceMetrics;
  
  /**
   * Create a new ImpalaFragmentMetrics map.
   * @param metricsMetadata
   * @param instanceMetricsMap - this is a map from instance to the metrics for
   * the instance. The metrics for the instance are modeled as a map from
   * metric name to metric value.
   */
  public ImpalaFragmentMetrics(
      List<ImpalaFragmentMetric> metricsMetadata,
      Map<String, Map<String, Double>> instanceMetricsMap) {
    Preconditions.checkNotNull(metricsMetadata);
    Preconditions.checkNotNull(instanceMetricsMap);
    this.metricsMetadata = metricsMetadata;
    instanceMetrics = Lists.newArrayList();
    for (Entry<String, Map<String, Double>> entry : instanceMetricsMap.entrySet()) {
      instanceMetrics.add(
          new ImpalaInstanceMetrics(
              translateToId("instance-" + entry.getKey()),
              entry.getValue()));
    }
  }

  public List<ImpalaFragmentMetric> getMetricsMetadata() {
    return metricsMetadata;
  }
  
  public List<ImpalaInstanceMetrics> getInstanceMetrics() {
    return instanceMetrics;
  }

  /**
   * Translate the id into something that's usable by the front-end.
   * @param str
   * @return
   */
  private String translateToId(String str) {
    return str.toLowerCase().replace(" ", "-").replace(":", "-");
  }
}
