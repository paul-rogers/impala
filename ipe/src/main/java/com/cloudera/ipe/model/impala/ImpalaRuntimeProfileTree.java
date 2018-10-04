// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TTimeSeriesCounter;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.cloudera.ipe.util.ImpalaRuntimeProfileUtils;
import com.cloudera.ipe.util.JodaUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This is a different view of the impala TRuntimeProfileTree thrift object.
 * The difference is that while the thrift object has all the nodes in a
 * pre-order traversal list and this models it as an actual tree structure.
 * Also, this view supports a lot of helper functions for easier traversal
 * of the tree and analysis of nodes.
 *
 * This class has certain semantics related to error handling.
 * 1. It doesn't handle malformed queryIds or fields (ie throws if it sees a
 * null where it doesn't expect it)
 * 2. It will try to handle missing nodes or info_strings gracefully, either
 * by returning null or an empty list where appropriate.
 */
public class ImpalaRuntimeProfileTree {
  public static final DateTimeFormatter MILLISECOND_TIME_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(
          JodaUtil.TZ_DEFAULT);

  private final ImpalaRuntimeProfileNode root;
  private final TRuntimeProfileTree tree;

  /**
   * Create an ImpalaRuntimeProfileTree. The tree must at least have a root node.
   * @param root
   * @param tree
   */
  public ImpalaRuntimeProfileTree(
      final ImpalaRuntimeProfileNode root,
      final TRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    Preconditions.checkNotNull(root);
    Preconditions.checkArgument(!tree.getNodes().isEmpty());
    this.root = root;
    this.tree = tree;
  }

  public ImpalaRuntimeProfileNode getRoot() {
    return root;
  }

  public TRuntimeProfileTree getThriftTree() {
    return tree;
  }

  public List<ImpalaRuntimeProfileFragmentNode> getFragments() {
    ImpalaRuntimeProfileNode fragmentRoot = getExecutionProfileNode();
    if (fragmentRoot == null) {
      return ImmutableList.<ImpalaRuntimeProfileFragmentNode>of();
    }
    return getFragmentsFromFragmentRoot(fragmentRoot);
  }

  public Long getRowsProduced() {
    // There are two cases for rows produced - select queries and insert queries
    String queryType = getSummaryMap().get(IPEConstants.IMPALA_QUERY_TYPE_INFO_STRING);
    // In select queries we get the rows produced from the execution node's child
    if (StringUtils.equalsIgnoreCase(queryType, IPEConstants.IMPALA_QUERY_TYPE_QUERY)) {
      ImpalaRuntimeProfileCoordinatorNode coordinator = getCoordinatorNode();
      if (coordinator != null) {
        return coordinator.getRowsProduced();
      }
      return null;
    }
    // For insert queries we get it by summing together the counts from
    // each of the fragment0 instance nodes.
    if (getFragments().size() == 0) {
      return null;
    }
    long total = 0;
    ImpalaRuntimeProfileFragmentNode fragment0 = getFragments().get(0);
    for (ImpalaRuntimeProfileInstanceNode instance : fragment0.getInstances()) {
      TCounter counter =
          instance.findCounterWithName(IPEConstants.IMPALA_PROFILE_ROWS_PRODUCED);
      if (counter != null) {
        total += counter.getValue();
      }
    }
    return total;
  }

  /**
   * Extract the queryId from the root tree node.
   * @return
   * @throw if the the queryId format is incorrect
   */
  public String getQueryId() {
    // The queryId is in the first node
    // The format is Query (id=1ac8d8e27d2a4ba2:908e92f32c2f346f)
    TRuntimeProfileNode queryIdNode = root.getThriftNode();
    if (queryIdNode.getName() == null) {
      throw new ImpalaCorruptProfileException(
          "Profile has a null queryId");
    }
    if (queryIdNode.getName().length() < 12) {
      throw new ImpalaCorruptProfileException(
          "Profile queryId is too short: " + queryIdNode.getName());
    }
    return queryIdNode.getName().substring(10,
                                           queryIdNode.getName().length() - 1);
  }

  /**
   * Returns the impala query plan. Note that this can be null.
   * @return
   */
  public String getQueryPlan() {
    return getSummaryMap().get(IPEConstants.IMPALA_QUERY_PLAN);
  }

  /**
   * How we determine the query's duration depends on a few things. See the
   * comments in the function itself for the various cases. If we can't
   * determine the duration then we return null.
   * @return
   */
  public Duration getDuration(List<DateTimeFormatter> timeFormats) {
    return new Duration(getStartTime(timeFormats, MILLISECOND_TIME_FORMATTER),
                        getEndTime(timeFormats, MILLISECOND_TIME_FORMATTER));
  }

  public Instant getStartTime(final List<DateTimeFormatter> timeFormats,
      final DateTimeFormatter millisecondTimeFormatter) {
    return parseTime(getSummaryMap().get(IPEConstants.IMPALA_START_TIME_INFO_STRING),
                     timeFormats,
                     millisecondTimeFormatter);
  }

  public Instant getStartTime(final List<DateTimeFormatter> timeFormats) {
    return getStartTime(timeFormats, MILLISECOND_TIME_FORMATTER);
  }

  /**
   * Returns whether an end time info string exists, without trying to parse
   * it.
   * @return
   */
  public boolean hasEndTime() {
    return StringUtils.isNotEmpty(
        getSummaryMap().get(IPEConstants.IMPALA_END_TIME_INFO_STRING));
  }

  public Instant getEndTime(final List<DateTimeFormatter> timeFormats,
      final DateTimeFormatter millisecondTimeFormatter) {
    return parseTime(getSummaryMap().get(IPEConstants.IMPALA_END_TIME_INFO_STRING),
                     timeFormats,
                     millisecondTimeFormatter);
  }

  public Instant getEndTime(final List<DateTimeFormatter> timeFormats) {
    return getEndTime(timeFormats, MILLISECOND_TIME_FORMATTER);
  }

  /**
   * Parses the time string. Returns null if the time format isn't correct.
   * @param timeString
   * @param timeFormats - the time format we will will try to use to do the
   * conversion.
   * @param millisecondTimeFormatter -- exposed for testing purposes, this
   * should always be set to MILLISECOND_TIME_FORMATTER at runtime.
   * @return
   */
  private static Instant parseTime(
      final String timeString,
      final List<DateTimeFormatter> timeFormats,
      final DateTimeFormatter millisecondTimeFormatter) {
    if (timeString == null) {
      return null;
    }
    for (DateTimeFormatter format : timeFormats) {
      try {
        return Instant.parse(timeString, format);
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
    // If we got we need to handle the nanosecond case. Joda cannot parse
    // 2013-06-27 13:58:25.064797000 because it doesn't know what to do with
    // the nanoseconds. To handle this we chop off the last 6 digits and
    // parse it as the milliescond time format.
    if (timeString.length() > 6) {
      try {
      return Instant.parse(timeString.substring(0, timeString.length() - 6),
                           millisecondTimeFormatter);
      } catch (IllegalArgumentException e) {
        // Continue through.
      }
    }
    return null;
  }

  /**
   * Returns the info strings map from the summary node in the runtime profile.
   * ImpalaCorruptProfileException if the infoStrings object is null. It is
   * possible that the any of the keys / values in the map are null.
   * @return
   */
  public Map<String, String> getSummaryMap() {
    ImpalaRuntimeProfileNode summaryNode = getSummaryNode();
    if (summaryNode == null) {
      return Maps.newHashMap();
    }
    Map<String, String> ret = getSummaryNode().getThriftNode().getInfo_strings();
    // Make sure there are no null-values anywhere
    if (ret == null) {
      throw new ImpalaCorruptProfileException("Summary node has no info strings");
    }
    return ret;
  }

  /**
   * Returns whether this is a complete enough profile for us to want to store
   * it and show it to the user.
   * @return
   */
  public boolean isWellFormed(final List<DateTimeFormatter> timeFormats) {
    return tree.getNodes().size() >= 2 &&
           haveAllSummaryNodeEntries() &&
           getDuration(timeFormats) != null;
  }

  /**
   * Checks to see if we have all the summary node entries we expect. Some
   * Impala queries don't set all of them.
   * @return
   */
  private boolean haveAllSummaryNodeEntries() {
    ImpalaRuntimeProfileNode summaryNode = getSummaryNode();
    if (summaryNode == null) {
      return false;
    }
    Map<String, String> summaryInfo = summaryNode.getThriftNode().getInfo_strings();
    return
        summaryInfo.containsKey(IPEConstants.IMPALA_START_TIME_INFO_STRING) &&
        summaryInfo.containsKey(IPEConstants.IMPALA_SQL_STATEMENT_INFO_STRING) &&
        summaryInfo.containsKey(IPEConstants.IMPALA_QUERY_TYPE_INFO_STRING) &&
        summaryInfo.containsKey(IPEConstants.IMPALA_QUERY_STATE_INFO_STRING) &&
        summaryInfo.containsKey(IPEConstants.IMPALA_USER_INFO_STRING) &&
        summaryInfo.containsKey(IPEConstants.IMPALA_DEFAULT_DB_INFO_STRING);
  }

  /**
   * Returns the summary node or null if it can't find it.
   * @return
   */
  public ImpalaRuntimeProfileNode getSummaryNode() {
    if (root.getChildren().size() == 0) {
      return null;
    }
    return root.getChildren().get(0);
  }

  /**
   * Returns the execution profile node or null if it can't find it.
   */
  public ImpalaRuntimeProfileNode getExecutionProfileNode() {
    if (root.getChildren().size() < 3) {
      return null;
    }
    return root.getChildren().get(2);
  }

  /**
   * Returns the impala server node or null if it can't find it.
   */
  public ImpalaRuntimeProfileNode getImpalaServerNode() {
    if (root.getChildren().size() < 2) {
      return null;
    }
    return root.getChildren().get(1);
  }

  /**
   * Returns the coordinator node, which will be one of the top-level execution profile
   * nodes. Returns null if the coordinator node is not present.
   */
  public ImpalaRuntimeProfileCoordinatorNode getCoordinatorNode() {
    ImpalaRuntimeProfileNode executionNode = getExecutionProfileNode();
    if (executionNode == null) {
      return null;
    }
    for (ImpalaRuntimeProfileNode childNode : executionNode.getChildren()) {
      if (childNode instanceof ImpalaRuntimeProfileCoordinatorNode) {
        return (ImpalaRuntimeProfileCoordinatorNode) childNode;
      }
    }
    return null;
  }

  private List<ImpalaRuntimeProfileFragmentNode> getFragmentsFromFragmentRoot(
      final ImpalaRuntimeProfileNode fragmentRoot) {
    List<ImpalaRuntimeProfileFragmentNode> ret = Lists.newArrayList();
    for (ImpalaRuntimeProfileNode node : fragmentRoot.getChildren()) {
      if (node instanceof ImpalaRuntimeProfileFragmentNode) {
        ret.add((ImpalaRuntimeProfileFragmentNode)node);
      }
    }
    return ret;
  }

  /**
   * Returns the sum of all counter values with the specified names in nodes
   * with the specified prefix. Note that this will also check for the counter
   * in the coordinator node, on the presumption that that has the counter it
   * is worth including. For example this is used to sum all the bytes_read
   * counters from HDFS_SCAN_NODE nodes. If no counter is found with the given
   * name, then null is returned.
   *
   * Any floating-point counter values are rounded before adding to the sum.
   */
  public Long getSumAllCounterValues(Set<String> counterNames,
      String nodePrefix) {
    List<TCounter> counters = Lists.newArrayList();
    collectCountersWithName(counterNames, nodePrefix, counters);
    return ImpalaRuntimeProfileUtils.sumLongCounters(counters);
  }

  /**
   * Convenience version of getSumAllCounterValues() for a single counter name.
   */
  public Long getSumAllCounterValues(String counterName, String nodePrefix) {
    return getSumAllCounterValues(
        Collections.singleton(counterName),
        nodePrefix);
  }

  /**
   * Similar to {@link #getSumAllCounterValues(Set<String>, String)}, except
   * that it computes the maximum value.
   */
  public Long getMaxAllCounterValues(String counterName, String nodePrefix) {
    List<TCounter> counters = Lists.newArrayList();
    collectCountersWithName(
        Collections.singleton(counterName),
        nodePrefix,
        counters);
    return ImpalaRuntimeProfileUtils.maxLongCounters(counters);
  }

  /**
   * Similar to {@link #getSumAllCounterValues(Set<String>, String)}, except
   * that it computes the minimum value.
   */
  public Long getMinAllCounterValues(String counterName, String nodePrefix) {
    List<TCounter> counters = Lists.newArrayList();
    collectCountersWithName(
        Collections.singleton(counterName),
        nodePrefix,
        counters);
    return ImpalaRuntimeProfileUtils.minLongCounters(counters);
  }

  /**
   * Similar to {@link #getSumAllCounterValues(Set<String>, String)}, except
   * that it computes the average value.
   */
  public Double getAvgAllCounterValues(String counterName, String nodePrefix) {
    List<TCounter> counters = Lists.newArrayList();
    collectCountersWithName(
        Collections.singleton(counterName),
        nodePrefix,
        counters);
    if (counters.size() == 0) {
      return null;
    }
    return ((double) ImpalaRuntimeProfileUtils.sumLongCounters(counters))
        / counters.size();
  }

  /**
   * Collect all counters for each distinct node type in the profile.
   */
  public Map<String, ImpalaNodewiseMetric> collectNodewiseMetrics() {
    ImpalaRuntimeProfileCoordinatorNode coordinatorNode = getCoordinatorNode();
    // do not use immutable map, as there could be duplicates across coordinator
    // and fragment nodes
    Map<String, ImpalaNodewiseMetric> nodewiseMetrics = Maps.newHashMap();
    List<ImpalaRuntimeProfileFragmentNode> fragments = getFragments();
    if (coordinatorNode != null) {
      nodewiseMetrics.putAll(coordinatorNode.collectNodewiseMetrics());
    }
    for (ImpalaRuntimeProfileFragmentNode fragment : fragments) {
      nodewiseMetrics.putAll(fragment.collectNodewiseMetrics());
    }
    return nodewiseMetrics;
  }

  /**
   * Collect all counters in the tree with the specific names in nodes with the specified
   * prefix into the provided list, including in the coordinator node.
   */
  public void collectCountersWithName(Set<String> counterNames, String nodePrefix,
      List<TCounter> counters) {
    ImpalaRuntimeProfileNode executionNode = getExecutionProfileNode();
    if (executionNode != null) {
      if (executionNode.getChildren().size() > 0) {
        ImpalaRuntimeProfileCoordinatorNode coordinatorNode = getCoordinatorNode();
        if (coordinatorNode != null) {
          coordinatorNode.collectCountersWithNameRecursive(counterNames, nodePrefix, counters);
        }
      }
    }
    List<ImpalaRuntimeProfileFragmentNode> fragments = getFragments();
    for (ImpalaRuntimeProfileFragmentNode fragment : fragments) {
      List<ImpalaRuntimeProfileInstanceNode> instances = fragment.getInstances();
      for (ImpalaRuntimeProfileInstanceNode node : instances) {
        node.collectCountersWithNameRecursive(counterNames, nodePrefix, counters);
      }
    }
  }

  /**
   * Convenience version of collectCountersWithName() for a single counter name.
   */
  public void collectCountersWithName(String counterName, String nodePrefix,
      List<TCounter> counters) {
    Preconditions.checkNotNull(counterName);
    collectCountersWithName(Collections.singleton(counterName), nodePrefix, counters);
  }

  /**
   * Returns a pretty-printed string of the profile
   * @return
   */
  public String getPrettyProfile(ImpalaHumanizer humanizer) {
    return root.getPrettyNodeBuilder(humanizer, "").toString();
  }

  /**
   * Return the event sequence addressed by name of the sequence
   * @param index
   * @return
   */
  public ImpalaRuntimeProfileEventSequence getEventSequence(String index) {
    Preconditions.checkNotNull(index);
    ImpalaRuntimeProfileNode summaryNode = getSummaryNode();
    if (summaryNode == null) {
      return null;
    }
    List<ImpalaRuntimeProfileEventSequence> sequences =
        summaryNode.getEventSequences();
    for (Iterator<ImpalaRuntimeProfileEventSequence> it = sequences.iterator();
            it.hasNext();) {
      ImpalaRuntimeProfileEventSequence seq = it.next();

      if (seq.getName().equals(index)) {
        return seq;
      }
    }
    return null;
  }

  /**
   * Returns the "Query Timeline" event sequence
   */
  public ImpalaRuntimeProfileEventSequence getQueryTimeline() {
    return getEventSequence(IPEConstants.QUERY_TIMELINE_INDEX);
  }

  /**
   * Returns the "Planner Timeline" event sequence
   */
  public ImpalaRuntimeProfileEventSequence getPlannerTimeline() {
    return getEventSequence(IPEConstants.PLANNER_TIMELINE_INDEX);
  }

  /**
   * Small utility struct to hold a hostname and counter pair. Note that we
   * put __coordinator__ in for hostname occasionally.
   */
  public static class HostAndCounter {
    public final String hostname;
    public final TTimeSeriesCounter counter;

    public HostAndCounter(String hostname,
                          TTimeSeriesCounter counter) {
      Preconditions.checkNotNull(hostname);
      Preconditions.checkNotNull(counter);
      this.hostname = hostname;
      this.counter = counter;
    }
  }
  /**
   * Returns the list of all time series counters for this query. Every fragment
   * instance has one time series stream per metric.
   * @param metric
   * @return
   */
  public List<HostAndCounter> getAllTimeSeries(String metricName) {
    List<HostAndCounter> allTimeSeries = Lists.newArrayList();
    // The coordinator node, if it exists, is the only instance of "fragment 0",
    // so grab that as well as all the normal fragments instances.
    ImpalaRuntimeProfileNode coordinatorNode = getCoordinatorNode();
    if (coordinatorNode != null) {
      TTimeSeriesCounter counter = coordinatorNode.getTimeSeriesCounter(metricName);
      if (counter != null) {
        allTimeSeries.add(new HostAndCounter("__coordinator__", counter));
      }
    }
    for (ImpalaRuntimeProfileFragmentNode fragment : getFragments()) {
      for (ImpalaRuntimeProfileInstanceNode instance : fragment.getInstances()) {
        TTimeSeriesCounter counter = instance.getTimeSeriesCounter(metricName);
        if (counter != null) {
          allTimeSeries.add(new HostAndCounter(instance.getHostname(),
                                               counter));
        }
      }
    }
    return allTimeSeries;
  }

  /**
   * Return the "wait time" of the input event. That is, the amount of time
   * between the event and the previous event in the sequence. Since Impala stores
   * multiple event sequences, the index parameter is used to identify the correct one.
   * @param eventName
   * @param index
   * @return
   */
  public Duration getEventWaitTime(final String eventName, String index) {
    Preconditions.checkNotNull(eventName);
    ImpalaRuntimeProfileEventSequence events = getEventSequence(index);
    if (events == null) {
      return null;
    }
    long previousEventTimestamp = 0L;
    for (ImpalaRuntimeProfileEvent event : events.getEvents()) {
      if (event.getLabel().equals(eventName)) {
        long nanos = event.getTimestamp() - previousEventTimestamp;
        return new Duration(nanos / 1000000);
      }
      previousEventTimestamp = event.getTimestamp();
    }
    return null;
  }

  /**
   * Compute the average HDFS scan range for the query, ignoring nodes that
   * scanned only a single range. Nong suggested we ignore nodes with a single
   * scan range since it usually means reading from a tiny table, which is
   * common, and we don't want it throwing off the average of the larger scans.
   * @return
   */
  public Double getAverageScanRange() {
    boolean found = false;
    long bytesRead = 0;
    long scanRanges = 0;
    for (ImpalaRuntimeProfileFragmentNode fragment : getFragments()) {
      for (ImpalaRuntimeProfileInstanceNode node : fragment.getInstances()) {
        if (node.getThriftNode().getName().startsWith(
            IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX)) {
          TCounter bytes =
              node.findCounterWithName(IPEConstants.IMPALA_PROFILE_BYTES_READ);
          TCounter ranges =
              node.findCounterWithName(IPEConstants.IMPALA_PROFILE_SCAN_RANGES);
          if (bytes == null || ranges == null || ranges.getValue() <= 0) {
            continue;
          }
          bytesRead += bytes.getValue();
          scanRanges += ranges.getValue();
          found = true;
        }
        for (ImpalaRuntimeProfileNode child : node.getAllChildren()) {
          if (child.getName().startsWith(
              IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX)) {
            TCounter bytes =
                child.findCounterWithName(IPEConstants.IMPALA_PROFILE_BYTES_READ);
            TCounter ranges =
                child.findCounterWithName(IPEConstants.IMPALA_PROFILE_SCAN_RANGES);
            if (bytes == null || ranges == null || ranges.getValue() <= 0) {
              continue;
            }
            bytesRead += bytes.getValue();
            scanRanges += ranges.getValue();
            found = true;
          }
        }
      }
    }
    ImpalaRuntimeProfileCoordinatorNode coordinatorNode = getCoordinatorNode();
    if (null != coordinatorNode) {
      Double bytes = coordinatorNode.sumCountersWithNameRecursive(
          IPEConstants.IMPALA_PROFILE_BYTES_READ,
          IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
      Double ranges = coordinatorNode.sumCountersWithNameRecursive(
          IPEConstants.IMPALA_PROFILE_SCAN_RANGES,
          IPEConstants.IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX);
      if (bytes != null && null != ranges) {
        bytesRead += bytes;
        scanRanges += ranges;
        found = true;
      }
    }
    if (found) {
      return (double) bytesRead / scanRanges;
    } else {
      return null;
    }
  }

}
