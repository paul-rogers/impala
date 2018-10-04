// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe;

import com.google.common.base.Preconditions;

import org.joda.time.Instant;

/**
 * Data structure to represent a metric with the associated timestamp.
 */
public class TimedDataPoint {

  private Instant timestamp;
  private Double value;

  public TimedDataPoint(Instant timestamp, double value) {
    this.timestamp = Preconditions.checkNotNull(timestamp);
    this.value = value;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public Double getValue() {
    return value;
  }

}
