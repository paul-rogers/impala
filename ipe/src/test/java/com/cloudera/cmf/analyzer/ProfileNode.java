package com.cloudera.cmf.analyzer;

import java.util.List;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Facade for a profile node. Wraps the profile node to provide convenience
 * accessors. Subclasses provide enums to access attributes and counters.
 * Subclasses also parse out and organize child nodes to convert the
 * generic structure of the profile into a meaningful set of query
 * structures.
 */
public class ProfileNode {

  public static class NodeIndex {
    int index;

    public NodeIndex(int index) {
      this.index = index;
    }
  }

  /**
   * Generic facade for profile nodes with no useful
   * content.
   */
  public static class HelperPNode extends ProfileNode {

    public HelperPNode(ProfileFacade analyzer, int index) {
      super(analyzer, index);
      Preconditions.checkState(childCount() == 0);
    }
  }

  protected final ProfileFacade analyzer;
  protected final TRuntimeProfileNode node;
  protected final int index;
  protected int firstChild;

  public ProfileNode(ProfileFacade analyzer, int index) {
    this.analyzer = analyzer;
    this.index = index;
    node = analyzer.node(index);
    firstChild = index + 1;
  }

  public TRuntimeProfileNode node() { return node; }

  public String attrib(String key) {
    return node.getInfo_strings().get(key);
  }

  public String name() {
    return node.getName();
  }

  public int childCount() {
    return node.getNum_children();
  }

  public String genericName() {
    return node.getName();
  }

  public List<ProfileNode> childNodes() {
    return Lists.newArrayList();
  }

  public long counter(String name) {
    // TODO: Cache counters in a map?
    for (TCounter counter : node.getCounters()) {
      if (counter.getName().equals(name)) {
        return counter.getValue();
      }
    }
    return 0;
  }

  public TEventSequence events(String name) {
    // Used in only one node, only two sequences.
    // Linear search is fine.
    for (TEventSequence event : node.getEvent_sequences()) {
      if (event.getName().equals(name)) {
        return event;
      }
    }
    return null;
  }

  public TTimeSeriesCounter timeSeries(String name) {
    // Used in only a few nodes, only two sequences.
    // Linear search is fine.
    for (TTimeSeriesCounter event : node.getTime_series_counters()) {
      if (event.getName().equals(name)) {
        return event;
      }
    }
    return null;
  }
}