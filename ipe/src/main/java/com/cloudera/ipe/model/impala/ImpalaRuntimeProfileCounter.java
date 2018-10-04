// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TUnit;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * A wrapper class for an impala runtime profile counter. Supports getting
 * the child counters and can be ordered alphabetically by counter name.
 */
public class ImpalaRuntimeProfileCounter
    implements Comparable<ImpalaRuntimeProfileCounter> {
  private final TCounter counter;
  private final ImmutableSet<ImpalaRuntimeProfileCounter> children;

  public ImpalaRuntimeProfileCounter(
      final TCounter counter,
      final ImmutableSet<ImpalaRuntimeProfileCounter> children) {
    Preconditions.checkNotNull(counter);
    Preconditions.checkNotNull(children);
    if (counter.getName() == null) {
      throw new ImpalaCorruptProfileException("Counter has no name");
    }
    this.counter = counter;
    this.children = children;
  }

  public TCounter getCounter() {
    return this.counter;
  }

  public Boolean hasChildren() {
    return children.size() > 0;
  }

  public ImmutableSet<ImpalaRuntimeProfileCounter> getChildren() {
    return children;
  }

  /**
   * Counters in the profile should be ordered alphabetically be their name.
   */
  @Override
  public int compareTo(ImpalaRuntimeProfileCounter other) {
    if (other == null) {
      return 1;
    }
    return counter.getName().compareTo(other.getCounter().getName());
  }

  /**
   * Returns a StringBuilder populated with a pretty printed profile counter.
   * Note that this pretty-printed profile returns both the humanized counter value
   * and the raw counter value.
   * @param indentationLevel
   * @return
   */
  StringBuilder getPrettyCounterBuilder(ImpalaHumanizer humanizer, String indentation) {
    Preconditions.checkNotNull(indentation);
    // Pretty print itself in the format:
    //    '<indentation>- <name>: <humanized value> (<raw value>)\n'
    StringBuilder builder = new StringBuilder();
    builder.append(indentation);
    builder.append("- ");
    builder.append(counter.getName());
    builder.append(": ");
    builder.append(humanizer.humanizeCounter(this));
    builder.append(" (");
    // Print the raw DOUBLE value: the long representation is not meaningful.
    if (counter.getUnit().equals(TUnit.DOUBLE_VALUE)) {
      builder.append(Double.longBitsToDouble(counter.getValue()));
    } else {
      builder.append(counter.getValue());
    }
    builder.append(")");
    builder.append("\n");
    for (ImpalaRuntimeProfileCounter child : children) {
      builder.append(child.getPrettyCounterBuilder(humanizer, indentation + "  "));
    }
    return builder;
  }
}
