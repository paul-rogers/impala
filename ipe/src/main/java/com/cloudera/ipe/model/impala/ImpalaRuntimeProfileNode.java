// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.util.ImpalaRuntimeProfileUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A node in the Impala runtime profile tree.
 */
public class ImpalaRuntimeProfileNode {

  private final static Logger LOG =
      LoggerFactory.getLogger(ImpalaRuntimeProfileNode.class);

  private static final String ROOT_COUNTER = "";

  protected final TRuntimeProfileNode thriftNode;
  protected ImmutableList<ImpalaRuntimeProfileNode> children;
  protected final ImpalaRuntimeProfileNode parent;
  protected final ImmutableSet<ImpalaRuntimeProfileCounter> rootCounters;
  protected final Map<String, TTimeSeriesCounter> timeSeriesMap;

  public ImpalaRuntimeProfileNode(
      final TRuntimeProfileNode node,
      final ImpalaRuntimeProfileNode parent) {
    Preconditions.checkNotNull(node);
    this.thriftNode = node;
    this.parent = parent;
    timeSeriesMap = Maps.newHashMap();
    if (thriftNode.getTime_series_counters() != null) {
      for (TTimeSeriesCounter ts : thriftNode.getTime_series_counters()) {
        timeSeriesMap.put(ts.getName(), ts);
      }
    }
    // The way Impala stores child counters is a little awkward, but this code
    // roughly imitates the code they use when they're processing the profile.
    //
    // Their child map looks something like this:
    // {"" : ["root1", "root2"], "root1": ["child1", "child2", child3"]}
    // We process this by first creating a map from the names to the thrift counter
    // object and then going through the map recursively, starting with "" (the
    // root) to build the child counters.
    Map<String, TCounter> nameToThriftCounter = Maps.newHashMap();
    for (TCounter counter : thriftNode.getCounters()) {
      nameToThriftCounter.put(counter.getName(), counter);
    }
    Set<String> rootNames = thriftNode.getChild_counters_map().get(ROOT_COUNTER);
    if (rootNames != null) {
      ImmutableSet.Builder<ImpalaRuntimeProfileCounter> rootCountersBuilder =
          nestedCountersHelper(
              thriftNode.getChild_counters_map().get(""),
              nameToThriftCounter);
      // This is a bit of a hack, but Impala doesn't add some counters to the
      // child map, notably TotalTime. They don't do this because they explicitly
      // call out the TotalTime counter in another part of the profile so they
      // don't want to duplicate it in their counters list. We, on the other hand,
      // want to call it out, so let's just put it (and any other missing ones
      // in the root)
      for (TCounter thriftCounter : nameToThriftCounter.values()) {
        ImpalaRuntimeProfileCounter counter =
            new ImpalaRuntimeProfileCounter(
                thriftCounter, ImmutableSet.<ImpalaRuntimeProfileCounter>of());
        rootCountersBuilder.add(counter);
      }
      rootCounters = rootCountersBuilder.build();
    } else {
      rootCounters = ImmutableSet.of();
    }
  }

  /**
   * Gets all the children node for a profile, recursing down the structure.
   * Returns the children in depth-first order
   * @return
   */
  public List<ImpalaRuntimeProfileNode> getAllChildren() {
    List<ImpalaRuntimeProfileNode> ret = Lists.newArrayList();
    for (ImpalaRuntimeProfileNode child : children) {
      ret.addAll(child.getAllChildren());
      ret.add(child);
    }
    return ret;
  }

  /**
   * Gets the total time counter for this node or null if there is none. Returns
   * the time in nano seconds.
   * @return
   */
  public Long getTotalTime() {
    TCounter counter = findCounterWithName(IPEConstants.IMPALA_PROFILE_TOTAL_TIME);
    if (counter == null) {
      return null;
    }
    return counter.getValue();
  }

  /**
   * This is helper to handle nested counters that calls itself recursively.
   * It takes in a list of counter names and recursively creates their children.
   *
   * It returns a list of the ImpalaRuntimeProfileCounter objects for the node
   * names it passed it.
   * @param currentNames
   * @param seenNames
   * @param nameToThriftCounter
   */
  private ImmutableSet.Builder<ImpalaRuntimeProfileCounter> nestedCountersHelper(
      final Set<String> names,
      final Map<String, TCounter> nameToThriftCounter) {
    ImmutableSortedSet.Builder<ImpalaRuntimeProfileCounter> builder =
        ImmutableSortedSet.naturalOrder();
    for (String name : names) {
      // Build the child counter objects recursively
      Set<String> childNames = thriftNode.getChild_counters_map().get(name);
      ImmutableSet<ImpalaRuntimeProfileCounter> childCounters = null;
      if (childNames != null) {
        childCounters = nestedCountersHelper(childNames, nameToThriftCounter).build();
      } else {
        childCounters = ImmutableSet.of();
      }
      TCounter thriftCounter = nameToThriftCounter.get(name);
      if (thriftCounter == null) {
        LOG.warn("Corrupt profile, missing counter: " + name);
        continue;
      }
      // We remove the counter for two reasons:
      // 1. If we don't we risk getting in an infinite loop if a bad profile
      // has a counter map that loops to itself.
      // 2. We want to know if any counters weren't in the children map
      // (see comment when we build the root counters).
      nameToThriftCounter.remove(name);
      ImpalaRuntimeProfileCounter counter =
          new ImpalaRuntimeProfileCounter(thriftCounter, childCounters);
      builder.add(counter);
    }

    return builder;
  }

  /**
   * Set the children. Note that this can only be done once.
   * @param children
   */
  public void setChildren(ImmutableList<ImpalaRuntimeProfileNode> children) {
    if (children == null || this.children != null) {
      throw new UnsupportedOperationException();
    }
    this.children = children;
  }

  public ImmutableList<ImpalaRuntimeProfileNode> getChildren() {
    return children;
  }

  /**
   * Returns the parent of the node. If this is the root node this returns
   * null.
   * @return
   */
  public ImpalaRuntimeProfileNode getParent() {
    return parent;
  }

  public TRuntimeProfileNode getThriftNode() {
    return thriftNode;
  }

  public String getName() {
    return thriftNode.getName();
  }

  public Set<ImpalaRuntimeProfileCounter> getRootCounters() {
    return rootCounters;
  }

  /**
   * Returns the counter with the given name if it exists in this profile node.
   * Otherwise return null.
   */
  public TCounter findCounterWithName(String name) {
    return findCounterWithName(Collections.singleton(name));
  }

  /**
   * Returns a counter with one of the given names if one exists in this profile node.
   * Otherwise return null.
   */
  public TCounter findCounterWithName(Set<String> names) {
    for (TCounter counter : getThriftNode().getCounters()) {
      if (counter.getName() == null) {
        LOG.warn("Profile TCounter has null name");
        continue;
      }
      if (names.contains(counter.getName())) {
        return counter;
      }
    }
    return null;
  }

  /**
   * Returns all counters with one of the given names if one exists in this profile node.
   * Otherwise returns an empty list.
   */
  public List<TCounter> findCountersWithName(Set<String> names) {
    List<TCounter> ret = Lists.newArrayList();
    collectCountersWithName(names, ret);
    return ret;
  }

  /**
   * Collect all counters with one of the given names in this profile node into
   * the provided list.
   */
  public void collectCountersWithName(Set<String> names, List<TCounter> counters) {
    for (TCounter counter : getThriftNode().getCounters()) {
      if (counter.getName() == null) {
        LOG.warn("Profile TCounter has null name");
        continue;
      }
      if (names.contains(counter.getName())) {
        counters.add(counter);
      }
    }
  }

  /**
   * Find all counters than match one of the counter names, searching in this node and any
   * of its descendants with a name that starts with nodePrefix.
   */
  public List<TCounter> findCountersWithNameRecursive(Set<String> names,
          String nodePrefix) {
    Preconditions.checkNotNull(names);
    Preconditions.checkNotNull(nodePrefix);
    List<TCounter> ret = Lists.newArrayList();
    collectCountersWithNameRecursive(names, nodePrefix, ret);
    return ret;
  }

  /**
   * Convenience version of findCountersWithNameRecursive() for a single name.
   */
  public List<TCounter> findCountersWithNameRecursive(String counterName,
                                             String nodePrefix) {
    Preconditions.checkNotNull(counterName);
    return findCountersWithNameRecursive(Collections.singleton(counterName), nodePrefix);
  }

  /**
   * Collect all counters in the subtree that match one of the counter names
   * into the counters list, searching in this node and any of its descendants
   * with a name that starts with nodePrefix.
   */
  public void collectCountersWithNameRecursive(Set<String> names, String nodePrefix,
      List<TCounter> counters) {
    // Check if current node satisfy the prefix.
    if (getName().startsWith(nodePrefix)) {
      collectCountersWithName(names, counters);
    }
    // Check the children
    for (ImpalaRuntimeProfileNode child : getAllChildren()) {
      if (child.getName().startsWith(nodePrefix)) {
        child.collectCountersWithName(names, counters);
      }
    }
  }

  /**
   * Collect all counters for each distinct node type in this subtree.
   */
  public Map<String, ImpalaNodewiseMetric> collectNodewiseMetrics() {
    Map<String, ImpalaNodewiseMetric> nodewiseMetrics = new HashMap<String, ImpalaNodewiseMetric>();
    // do not use getInstances(), because it may be called by a coordinator node
    List<ImpalaRuntimeProfileInstanceNode> instances = Lists.newArrayList();
    for (ImpalaRuntimeProfileNode child : getChildren()) {
      if (child instanceof ImpalaRuntimeProfileInstanceNode) {
        instances.add((ImpalaRuntimeProfileInstanceNode) child);
      }
    }

    // walk through all instances to create the list.
    for (ImpalaRuntimeProfileInstanceNode instance : instances) {
      List<ImpalaRuntimeProfileNode> descendants = instance.getAllChildren();
      for (ImpalaRuntimeProfileNode node : descendants) {
        String nodeName = node.getName();
        if (!nodewiseMetrics.containsKey(nodeName)) {
          nodewiseMetrics.put(nodeName, new ImpalaNodewiseMetric(nodeName));
        }
        ImpalaNodewiseMetric nm = nodewiseMetrics.get(nodeName);
        for (TCounter counter : node.getThriftNode().getCounters()) {
          if (counter.getName() != null) {
            nm.addMetric(counter.getName(), ImpalaRuntimeProfileUtils.getDoubleValueFromCounter(counter));
          }
        }
      }
    }
    return nodewiseMetrics;
  }

  /**
   * Returns the sum of counters in the subtree matching this name,
   * across all the nodes that match the given prefix.
   * Null if no such counter exist.
   */
  public Double sumCountersWithNameRecursive(String counterName, String nodePrefix) {
    Preconditions.checkNotNull(counterName);
    return sumCountersWithNameRecursive(Collections.singleton(counterName), nodePrefix);
  }

  /**
   * Convenience version of sumCountersWithNameRecursive() for a single name.
   */
  public Double sumCountersWithNameRecursive(Set<String> counterNames, String nodePrefix) {
    Preconditions.checkNotNull(counterNames);
    Preconditions.checkNotNull(nodePrefix);
    List<TCounter> counters = findCountersWithNameRecursive(counterNames, nodePrefix);
    return ImpalaRuntimeProfileUtils.sumCounters(counters);
  }

  /**
   * Returns an iterator over the sorted collection of Impala info strings.
   */
  public ImmutableList<ImpalaRuntimeProfileInfoString> getInfoStrings() {
    ImmutableList.Builder<ImpalaRuntimeProfileInfoString> builder =
        ImmutableList.builder();
    // Add the info strings in the order defined.
    for (String infoKey : thriftNode.getInfo_strings_display_order()) {
      String infoValue = thriftNode.getInfo_strings().get(infoKey);
      if (infoValue == null) {
        LOG.warn("Corrupt profile, missing corresponding info " +
            "strings value for key: " + infoKey);
        continue;
      }
      ImpalaRuntimeProfileInfoString infoString =
          new ImpalaRuntimeProfileInfoString(infoKey,  infoValue);
      builder.add(infoString);
    }
    return builder.build();
  }

  /**
   * Get the list of event sequences. The list is built dynamically.
   * @return
   */
  public List<ImpalaRuntimeProfileEventSequence> getEventSequences() {
    if (thriftNode.getEvent_sequences() == null) {
      return ImmutableList.<ImpalaRuntimeProfileEventSequence>of();
    }
    List<ImpalaRuntimeProfileEventSequence> ret = Lists.newArrayList();
    for (TEventSequence sequence : thriftNode.getEvent_sequences()) {
      ret.add(new ImpalaRuntimeProfileEventSequence(sequence));
    }
    return ret;
  }

  /**
   * Returns a StringBuilder populated with a pretty printed profile node.
   * @return
   */
  StringBuilder getPrettyNodeBuilder(ImpalaHumanizer humanizer, String indentation) {
    Preconditions.checkNotNull(indentation);
    StringBuilder builder = new StringBuilder();
    // Add the name
    builder.append(indentation);
    builder.append(thriftNode.getName());
    builder.append("\n");

    // Add the info strings in the order defined
    for (ImpalaRuntimeProfileInfoString infoString : getInfoStrings()) {
      builder.append(indentation);
      builder.append("  ");
      builder.append(infoString.getKey());
      builder.append(": ");
      builder.append(infoString.getValue());
      builder.append("\n");
    }

    // Add the counters
    for (ImpalaRuntimeProfileCounter counter : rootCounters) {
      builder.append(counter.getPrettyCounterBuilder(humanizer, indentation + "  "));
    }
    for (ImpalaRuntimeProfileEventSequence sequence : getEventSequences()) {
      builder.append(sequence.getPrettyStringBuilder(humanizer, indentation + "  "));
    }
    // Add the children
    for (ImpalaRuntimeProfileNode node : children) {
      builder.append(node.getPrettyNodeBuilder(humanizer, indentation + "  "));
    }
    return builder;
  }

  /**
   * Returns the TTimeSeriesCounter object for the metric. Will return null
   * if the node doesn't have the metric.
   * @param metric
   * @return
   */
  public TTimeSeriesCounter getTimeSeriesCounter(String impalaMetricName) {
    if (impalaMetricName == null) {
      return null;
    }
    return timeSeriesMap.get(impalaMetricName);
  }
}
