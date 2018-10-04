// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import com.google.common.base.Preconditions;

/**
 * Our model of a <timestamp, label> tuple from the TEventSequence object
 */
public class ImpalaRuntimeProfileEvent {

  private final long timestamp;
  private final String label;

  public ImpalaRuntimeProfileEvent(final long timestamp, final String label) {
    Preconditions.checkNotNull(label);
    this.timestamp = timestamp;
    this.label = label;
  }

  /**
   * Return the time of the event in nanoseconds elapsed since the start of the query.
   * @return
   */
  public long getTimestamp() {
    return timestamp;
  }

  public String getLabel() {
    return label;
  }

  public StringBuilder getPrettyStringBuilder(ImpalaHumanizer humanizer, String indentation) {
    Preconditions.checkNotNull(indentation);
    return new StringBuilder().append(indentation)
        .append(label).append(": ").append(humanizer.humanizeNanoseconds(timestamp))
        .append(" (").append(timestamp).append(")")
        .append("\n");
  }
}
