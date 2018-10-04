// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TEventSequence;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Our model of a TEventSequence object.
 */
public class ImpalaRuntimeProfileEventSequence {

  private final String name;
  private final List<ImpalaRuntimeProfileEvent> events;
  
  public ImpalaRuntimeProfileEventSequence(
      final TEventSequence sequence) {
    Preconditions.checkNotNull(sequence);
    this.name = sequence.name;
    List<Long> timestamps = sequence.getTimestamps();
    List<String> labels = sequence.getLabels();
    if (timestamps.size() != labels.size()) {
      throw new ImpalaCorruptProfileException(
          "Event sequence label size, " + labels.size() +
          " does not match timestamp size, " + timestamps.size());
    }
    ImmutableList.Builder<ImpalaRuntimeProfileEvent> eventsBuilder =
        ImmutableList.builder();
    for (int i = 0; i < labels.size(); i++) {
      eventsBuilder.add(new ImpalaRuntimeProfileEvent(timestamps.get(i),
                                                      labels.get(i)));
    }
    this.events = eventsBuilder.build();
  }
  
  public String getName() {
    return name;
  }
  
  public List<ImpalaRuntimeProfileEvent> getEvents() {
    return events;
  }
  
  public StringBuilder getPrettyStringBuilder(ImpalaHumanizer humanizer, String indentation) {
    Preconditions.checkNotNull(indentation);
    StringBuilder builder = new StringBuilder(indentation);
    builder.append(name);
    builder.append("\n");
    for (ImpalaRuntimeProfileEvent event : events) {
      builder.append(event.getPrettyStringBuilder(humanizer, indentation + "  "));
    }
    return builder;
  }
}
