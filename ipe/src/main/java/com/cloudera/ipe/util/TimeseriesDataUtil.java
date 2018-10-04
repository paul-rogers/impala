// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.util;

import com.cloudera.ipe.IPEConstants;
import org.apache.impala.thrift.TTimeSeriesCounter;
import com.cloudera.ipe.TimedDataPoint;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree.HostAndCounter;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.joda.time.Instant;

public class TimeseriesDataUtil {

  /**
   * Returns time Sseries data. This works by getting all the time series streams
   * from an Impala runtime profile and merging them together.
   * @param tree
   * @param metric
   * @param startTime
   * @param endTime
   * @return
   */
  public static List<TimedDataPoint> getTimeSeriesData(
      final ImpalaRuntimeProfileTree tree,
      final String metricName,
      final Instant startTime,
      final Instant endTime) {
    Preconditions.checkNotNull(tree);
    Preconditions.checkNotNull(startTime);
    Preconditions.checkNotNull(endTime);
    List<HostAndCounter> counters = tree.getAllTimeSeries(metricName);
    // For certain metrics, like memory, we want to avoid adding in the
    // counter more than once from the same host at the same time since the
    // metric value is conceptually for the host and not just the instance.
    boolean oneValuePerHost = false;
    if (metricName.equals(IPEConstants.IMPALA_TIME_SERIES_COUNTER_MEMORY_USAGE)) {
      oneValuePerHost = true;
    }
    return mergeTimeSeries(
        tree.getStartTime(ImpalaRuntimeProfile.DEFAULT_TIME_FORMATS),
        startTime,
        endTime,
        counters,
        oneValuePerHost);
  }

  /**
   * This method merges multiple time series streams from an Impala runtime profile
   * into one stream representing the entire query.
   *
   * The tricky part of this is determining at what rate to sample the streams.
   * Streams have different end times, but the same start time (one stream can
   * be for a short period and have 20 points over 2 seconds, while another
   * stream can be over a long period and have 20 points over 5 minutes), To deal
   * with this, but still retain some of the precision of the faster sampling stream
   * we do the following: We start and the beginning of the Impala query (or
   * the tsquery start time if that's after the Impala query start time), then
   * we find the smallest shortest period of any stream that's still running
   * and we use that to determine the new sample point (lastSamplePoint +
   * shortestSamplePeriod).
   *
   * Note that streams don't always have exactly 20 points, but Impala's sampling
   * algorithm ensures that the number we get isn't too far from that.
   *
   * Also note that this can return an irregularly sampled time series (ie some
   * points may be one second apart where others are one minute apart).
   * @param impalaQueryStartTime
   * @param tsqueryStartTime
   * @param tsqueryEndTime
   * @param counters
   * @param oneValuePerHost
   * @return
   */
  @VisibleForTesting
  public
  static List<TimedDataPoint> mergeTimeSeries(
      final Instant impalaQueryStartTime,
      final Instant tsqueryStartTime,
      final Instant tsqueryEndTime,
      final List<HostAndCounter> counters,
      boolean oneValuePerHost) {
    Preconditions.checkNotNull(impalaQueryStartTime);
    Preconditions.checkNotNull(tsqueryStartTime);
    Preconditions.checkNotNull(tsqueryEndTime);
    Preconditions.checkNotNull(counters);
    Preconditions.checkState(!tsqueryEndTime.isBefore(impalaQueryStartTime));
    List<TimedDataPoint> ret = Lists.newArrayList();
    // Since the times in the streams are just relative to the start time
    // let's start by figuring out the relative start time to begin with.
    Instant currentTime = impalaQueryStartTime;
    if (tsqueryStartTime.isAfter(impalaQueryStartTime)) {
      currentTime = tsqueryStartTime;
    }
    // Make a copy so we don't modify the Thrift object
    List<HostAndCounter> tsCounters = Lists.newArrayList(counters);
    while (true) {
      // If the tsquery end time isn't after the current time then create the
      // last data point, at the tsquery end time. It should be safe to interpolate
      // to this point because Impala ensures that we're getting points at a
      // regular interval.
      boolean atEnd = false;
      if (!tsqueryEndTime.isAfter(currentTime)) {
        atEnd = true;
        currentTime = tsqueryEndTime;
      }
      double sumAtCurrentTime = 0;
      Integer shortestPeriod = null;
      Set<String> hostsSeen = Sets.newHashSet();
      // Go through all the streams we have left and add their value at that time.
      // Also figure out what the shortest period of any the remaining streams is.
      for (Iterator<HostAndCounter> it = tsCounters.iterator(); it.hasNext();) {
        HostAndCounter hostAndCounter = it.next();
        // For certain metrics, like memory, we want to avoid adding in the
        // counter more than once from the same host at the same time since the
        // metric value is conceptually for the host and not just the instance.
        if (oneValuePerHost && hostsSeen.contains(hostAndCounter.hostname)) {
          continue;
        }
        TTimeSeriesCounter counter = hostAndCounter.counter;
        long relativeOffsetMs = currentTime.getMillis() - impalaQueryStartTime.getMillis();
        Double point = getInterpolatedPoint(counter, relativeOffsetMs);
        if (point == null) {
          // We're past the end of this stream so let's remove it from the list
          it.remove();
        } else {
          sumAtCurrentTime += point;
          hostsSeen.add(hostAndCounter.hostname);
          if (shortestPeriod == null || counter.getPeriod_ms() < shortestPeriod) {
            shortestPeriod = counter.getPeriod_ms();
          }
        }
      }
      // If we don't have any streams that match the point then we're done
      if (tsCounters.isEmpty()) {
        break;
      }
      ret.add(new TimedDataPoint(currentTime, sumAtCurrentTime));
      if (atEnd) {
        break;
      }
      Preconditions.checkState(shortestPeriod != null);
      currentTime = currentTime.plus(shortestPeriod);
    }
    return ret;
  }

  /**
   * Returns the value of the time series stream at the specified offset relative
   * to the query start time. If the offset is after the stream then return null.
   * If the offset is between points in the stream then return a linearly
   * interpolated value.
   * @param counter
   * @param relativeOffsetMs
   * @return
   */
  static private Double getInterpolatedPoint(
      final TTimeSeriesCounter counter,
      final long relativeOffsetMs) {
    Preconditions.checkNotNull(counter);
    Preconditions.checkArgument(relativeOffsetMs >= 0);
    // Consider we have a counter period of 1sec and 4 data points,
    // then we have points at 0, 1, 2, 3 seconds.
    // Thus we have the following cases
    // 1. We're over the end (ie, the index > 3)
    // 2. We're at an exact point in which case we return that.
    // 3. We're somewhere in between, in which case we linearly interpolate.
    if (counter.getPeriod_ms() == 0) {
      return null;
    }
    int intIndex = (int)(relativeOffsetMs / counter.getPeriod_ms());
    double doubleIndex = (double)relativeOffsetMs / (double)counter.getPeriod_ms();

    // Case 1
    if (doubleIndex > counter.getValuesSize() - 1) {
      return null;
    }
    // Case 2
    // This value needs to be long, since it can be huge and below
    // we are comparing againts long
    long compute_period_ms = ((long)intIndex * (long)counter.getPeriod_ms());
    if (compute_period_ms == relativeOffsetMs) {
      return (double)counter.getValues().get(intIndex);
    }

    // Case 3
    // Weight between the two. For example if the the doubleIndex is 8.4 we
    // would find the 8th and 9th elements and give the 8th element at 0.6 weight
    // and the 9th element a 0.4 weight.
    double firstValue = counter.getValues().get(intIndex);
    double secondValue = counter.getValues().get(intIndex + 1);
    return firstValue * ((intIndex + 1) - doubleIndex) +
           secondValue * (doubleIndex - intIndex);
  }

}
