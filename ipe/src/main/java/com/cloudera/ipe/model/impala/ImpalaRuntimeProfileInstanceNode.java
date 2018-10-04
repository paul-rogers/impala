// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.cloudera.ipe.util.ImpalaRuntimeProfileUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An instance node of a runtime profile.
 */
public class ImpalaRuntimeProfileInstanceNode extends ImpalaRuntimeProfileNode {

  private final String hostname;
  private final int port;
  private final String instanceGUID;
  
  // The name looks something like:
  // Instance 41332bacedcc4743:a6fabaf5977c92b1 (host=nong-desktop:22002)
  private static final Pattern pattern = 
      Pattern.compile("Instance (\\S+) \\(host=(\\S+):(\\d+)\\)$");
  
  public ImpalaRuntimeProfileInstanceNode(TRuntimeProfileNode node,
      ImpalaRuntimeProfileNode parent) {
    super(node, parent);

    Matcher matcher = pattern.matcher(node.getName());
    if (matcher.matches()) {
      instanceGUID = matcher.group(1);
      hostname = matcher.group(2);
      port = Integer.parseInt(matcher.group(3));
    } else {
      throw new ImpalaCorruptProfileException(
          "Instance node name is in an unknown format: " + node.getName());
    }
  }
  
  public String getHostname() {
    return hostname;
  }
  
  public int getPort() {
    return port;
  }
  
  public String getInstanceGUID() {
    return instanceGUID;
  }

  /**
   * Find all counters in this node and its children than match one of the counter names
   * and node prefix.
   */
  public List<TCounter> findCountersWithNameRecursive(Set<String> counterNames, String nodePrefix) {
    Preconditions.checkNotNull(counterNames);
    Preconditions.checkNotNull(nodePrefix);
    List<TCounter> ret = Lists.newArrayList();
    // Check this node
    if (thriftNode.getName().startsWith(nodePrefix)) {
      for (TCounter counter : findCountersWithName(counterNames)) {
        ret.add(counter);
      }
    }
    // Check the children
    for (ImpalaRuntimeProfileNode child : getAllChildren()) {
      if (child.getName().startsWith(nodePrefix)) {
        for (TCounter counter : child.findCountersWithName(counterNames)) {
          ret.add(counter);
        }
      }
    }
    return ret;
  }

  /**
   * Convenience version of findMatchingCounters() for a single counter name.
   */
  public List<TCounter> findCountersWithNameRecursive(String counterName, String nodePrefix) {
    return findCountersWithNameRecursive(Collections.singleton(counterName), nodePrefix);
  }

  /**
   * Returns the value of this metric. Null if the metric does not exist.
   * @param metric
   * @return
   */
  public Double getMetric(ImpalaFragmentMetric metric) {
    Preconditions.checkNotNull(metric);
    return ImpalaRuntimeProfileUtils.sumCounters(
        findCountersWithNameRecursive(metric.getCounterName(), metric.getNodePrefix()));
  }
}
