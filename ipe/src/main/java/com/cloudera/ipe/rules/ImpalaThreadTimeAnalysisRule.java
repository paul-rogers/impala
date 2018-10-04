// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import org.apache.impala.thrift.TCounter;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.IPEUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;

/**
 * This analysis rule is intended to extract attributes related to thread
 * times.
 */
public class ImpalaThreadTimeAnalysisRule implements ImpalaAnalysisRule {

  public static final String THREAD_TOTAL_TIME =
      "thread_total_time";
  public static final String THREAD_CPU_TIME =
      "thread_cpu_time";
  public static final String THREAD_CPU_TIME_PERCENTAGE =
      "thread_cpu_time_percentage";
  public static final String THREAD_STORAGE_WAIT_TIME =
      "thread_storage_wait_time";
  public static final String THREAD_STORAGE_WAIT_TIME_PERCENTAGE =
      "thread_storage_wait_time_percentage";
  public static final String THREAD_NETWORK_SEND_WAIT_TIME =
      "thread_network_send_wait_time";
  public static final String THREAD_NETWORK_SEND_WAIT_TIME_PERCENTAGE =
      "thread_network_send_wait_time_percentage";
  public static final String THREAD_NETWORK_RECEIVE_WAIT_TIME =
      "thread_network_receive_wait_time";
  public static final String THREAD_NETWORK_RECEIVE_WAIT_TIME_PERCENTAGE =
      "thread_network_receive_wait_time_percentage";

  public static final String PLANNING_WAIT_TIME =
      "planning_wait_time";
  public static final String PLANNING_WAIT_TIME_PERCENTAGE =
      "planning_wait_time_percentage";
  public static final String CLIENT_FETCH_WAIT_TIME =
      "client_fetch_wait_time";
  public static final String CLIENT_FETCH_WAIT_TIME_PERCENTAGE =
      "client_fetch_wait_time_percentage";

  private ImmutableList<DateTimeFormatter> profileTimeFormatters = null;

  public ImpalaThreadTimeAnalysisRule(ImmutableList<DateTimeFormatter> profileTimeFormatters) {
    this.profileTimeFormatters = profileTimeFormatters;
  }

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();

    Long cpuTimeNanos = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_TOTAL_CPU_TIME,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
    if (cpuTimeNanos == null) {
      // Profile format since 5.10
      Long sysTimeNanos = tree.getSumAllCounterValues(
          IPEConstants.IMPALA_PROFILE_TOTAL_SYS_TIME,
          IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
      Long userTimeNanos = tree.getSumAllCounterValues(
          IPEConstants.IMPALA_PROFILE_TOTAL_USER_TIME,
          IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
      if (sysTimeNanos != null || userTimeNanos != null) {
        cpuTimeNanos = (sysTimeNanos == null ? 0 : sysTimeNanos) +
            (userTimeNanos == null ? 0 : userTimeNanos);
      }
    }
    Long storageWaitTimeNanos = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_TOTAL_STORAGE_WAIT_TIME,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
    Long networkSendWaitTimeNanos = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_TOTAL_NETWORK_SEND_TIME,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
    Long networkReceiveWaitTimeNanos = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_TOTAL_NETWORK_RECEIVE_TIME,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);

    Long totalTimeNanos = null;
    if (cpuTimeNanos != null &&
        storageWaitTimeNanos != null &&
        networkSendWaitTimeNanos != null &&
        networkReceiveWaitTimeNanos != null) {
      totalTimeNanos = cpuTimeNanos + storageWaitTimeNanos +
          networkSendWaitTimeNanos + networkReceiveWaitTimeNanos;
    }
    // Impala uses nanoseconds, but we return the time in milliseconds.
    if (totalTimeNanos != null) {
      b.put(THREAD_TOTAL_TIME,
          String.valueOf(totalTimeNanos / IPEConstants.NANOS_PER_MILLIS));
    }
    if (cpuTimeNanos != null) {
      b.put(THREAD_CPU_TIME,
          String.valueOf(cpuTimeNanos / IPEConstants.NANOS_PER_MILLIS));
      if (totalTimeNanos != null) {
        Long threadCpuPercentage =
            IPEUtils.computePercentage(cpuTimeNanos, totalTimeNanos);
        if (threadCpuPercentage != null) {
          b.put(THREAD_CPU_TIME_PERCENTAGE, String.valueOf(threadCpuPercentage));
        }
      }
    }
    if (storageWaitTimeNanos != null) {
      b.put(THREAD_STORAGE_WAIT_TIME,
          String.valueOf(storageWaitTimeNanos / IPEConstants.NANOS_PER_MILLIS));
      if (totalTimeNanos != null) {
        Long storageWaitPercentage = IPEUtils
            .computePercentage(storageWaitTimeNanos, totalTimeNanos);
        if (storageWaitPercentage != null) {
          b.put(THREAD_STORAGE_WAIT_TIME_PERCENTAGE,
                String.valueOf(storageWaitPercentage));
        }
      }
    }
    if (networkSendWaitTimeNanos != null) {
      b.put(THREAD_NETWORK_SEND_WAIT_TIME,
          String.valueOf(networkSendWaitTimeNanos / IPEConstants.NANOS_PER_MILLIS));
      if (totalTimeNanos != null) {
        Long networkWaitPercentage = IPEUtils
            .computePercentage(networkSendWaitTimeNanos, totalTimeNanos);
        if (networkWaitPercentage != null) {
          b.put(THREAD_NETWORK_SEND_WAIT_TIME_PERCENTAGE,
                String.valueOf(networkWaitPercentage));
        }
      }
    }
    if (networkReceiveWaitTimeNanos != null) {
      b.put(THREAD_NETWORK_RECEIVE_WAIT_TIME,
          String.valueOf(networkReceiveWaitTimeNanos / IPEConstants.NANOS_PER_MILLIS));
      if (totalTimeNanos != null) {
        Long networkWaitPercentage = IPEUtils.computePercentage(networkReceiveWaitTimeNanos, totalTimeNanos);
        if (networkWaitPercentage != null) {
          b.put(THREAD_NETWORK_RECEIVE_WAIT_TIME_PERCENTAGE,
                String.valueOf(networkWaitPercentage));
        }
      }
    }

    Duration queryDuration = tree.getDuration(profileTimeFormatters);
    Duration planningWaitTime =
        tree.getEventWaitTime(IPEConstants.IMPALA_PROFILE_EVENT_PLANNING_FINISHED,
                IPEConstants.QUERY_TIMELINE_INDEX);
    if (planningWaitTime != null) {
      b.put(PLANNING_WAIT_TIME, String.valueOf(planningWaitTime.getMillis()));

      if (queryDuration != null && queryDuration.getMillis() > 0) {
        Long planningWaitTimePercentage =
            IPEUtils.computePercentage(
                planningWaitTime.getMillis(),
                queryDuration.getMillis());
        if (planningWaitTimePercentage != null) {
          b.put(PLANNING_WAIT_TIME_PERCENTAGE,
              String.valueOf(planningWaitTimePercentage));
        }
      }
    }

    ImpalaRuntimeProfileNode serverNode = tree.getImpalaServerNode();
    if (serverNode != null) {
      TCounter counter = serverNode.findCounterWithName(
          IPEConstants.IMPALA_PROFILE_CLIENT_FETCH_WAIT_TIMER);
      if (counter != null) {
        long clientFetchWaitTimeMs = counter.getValue() / IPEConstants.NANOS_PER_MILLIS;
        b.put(CLIENT_FETCH_WAIT_TIME, String.valueOf(clientFetchWaitTimeMs));

        if (queryDuration != null && queryDuration.getMillis() > 0) {
          Long clientFetchWaitTimePercentage =
              IPEUtils.computePercentage(clientFetchWaitTimeMs,
                                              queryDuration.getMillis());
          if (clientFetchWaitTimePercentage != null) {
            b.put(CLIENT_FETCH_WAIT_TIME_PERCENTAGE,
                String.valueOf(clientFetchWaitTimePercentage));
          }
        }
      }
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata threadTotalTimeMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_TOTAL_TIME)
        .setDisplayNameKey("impala.analysis.thread_total_time.name")
        .setDescriptionKey("impala.analysis.thread_total_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata threadCpuTimeMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_CPU_TIME)
        .setDisplayNameKey("impala.analysis.thread_cpu_time.name")
        .setDescriptionKey("impala.analysis.thread_cpu_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata threadCpuTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_CPU_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.thread_cpu_time_percentage.name")
        .setDescriptionKey("impala.analysis.thread_cpu_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata threadStorageWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_STORAGE_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.thread_storage_wait_time.name")
        .setDescriptionKey("impala.analysis.thread_storage_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata threadStorageWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_STORAGE_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.thread_storage_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.thread_storage_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata threadNetworkSendWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_NETWORK_SEND_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.thread_network_send_wait_time.name")
        .setDescriptionKey("impala.analysis.thread_network_send_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata threadNetworkSendWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_NETWORK_SEND_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.thread_network_send_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.thread_network_send_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata threadNetworkReceiveWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_NETWORK_RECEIVE_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.thread_network_receive_wait_time.name")
        .setDescriptionKey("impala.analysis.thread_network_receive_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata threadNetworkReceiveWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(THREAD_NETWORK_RECEIVE_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.thread_network_receive_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.thread_network_receive_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata planningWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(PLANNING_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.planning_wait_time.name")
        .setDescriptionKey("impala.analysis.planning_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata planningWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(PLANNING_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.planning_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.planning_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
    AttributeMetadata clientFetchWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(CLIENT_FETCH_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.client_fetch_wait_time.name")
        .setDescriptionKey("impala.analysis.client_fetch_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata clientFetchWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(CLIENT_FETCH_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.client_fetch_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.client_fetch_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .setSupportsHistograms(true)
        .build();
   return ImmutableList.of(threadTotalTimeMetadata,
                           threadCpuTimeMetadata,
                           threadCpuTimePercentageMetadata,
                           threadStorageWaitTimeMetadata,
                           threadStorageWaitTimePercentageMetadata,
                           threadNetworkSendWaitTimeMetadata,
                           threadNetworkSendWaitTimePercentageMetadata,
                           threadNetworkReceiveWaitTimeMetadata,
                           threadNetworkReceiveWaitTimePercentageMetadata,
                           planningWaitTimeMetadata,
                           planningWaitTimePercentageMetadata,
                           clientFetchWaitTimeMetadata,
                           clientFetchWaitTimePercentageMetadata);
  }
}
