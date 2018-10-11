package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TTimeSeriesCounter;

import com.cloudera.cmf.profile.Attrib.Counter;
import com.google.common.collect.Lists;

/**
 * Facade for a profile node. Wraps the profile node to provide convenience
 * accessors. See {@link Attrib} for the attributes and counters available
 * for each node.
 */
public class ProfileNode {

  /**
   * Iterator over nodes when expanding a node tree.
   */
  public static class NodeIterator {
    TRuntimeProfileTree profile;
    int index;

    public NodeIterator(TRuntimeProfileTree profile, int index) {
      this.profile = profile;
      this.index = index;
    }

    public TRuntimeProfileNode node() {
      return profile.getNodes().get(index);
    }

    public void incr() { index++; }

    public TRuntimeProfileNode next() {
      TRuntimeProfileNode node = node();
      incr();
      return node;
    }

    public int index() { return index; }
  }

  protected final TRuntimeProfileNode node;
  protected final PNodeType nodeType;
  protected final int index;

  /**
   * Constructor for synthetic nodes created during analysis that
   * don't exist in the profile tree.
   * @param node
   * @param nodeType
   */
  public ProfileNode(TRuntimeProfileNode node, PNodeType nodeType) {
    this.node = node;
    this.index = -1;
    this.nodeType = nodeType;
  }

  public ProfileNode(NodeIterator nodeIndex, PNodeType nodeType) {
    this.node = nodeIndex.node();
    this.index = nodeIndex.index();
    this.nodeType = nodeType;
    nodeIndex.incr();
  }

  public TRuntimeProfileNode node() { return node; }
  public String name() { return node.getName(); }
  public int childCount() { return node.getNum_children(); }
  public String genericName() { return node.getName(); }
  public PNodeType nodeType() { return nodeType; }

  public List<ProfileNode> childNodes() {
    return Lists.newArrayList();
  }

  public String attrib(String key) {
    return node.getInfo_strings().get(key);
  }

  public static String getAttrib(TRuntimeProfileNode node, String key) {
    return node.getInfo_strings().get(key);
  }

  public long counter(Counter counter) {
    return Attrib.counter(node, counter);
  }

  public long counter(String name) {
    return Attrib.counter(node, name);
  }

  public TEventSequence events(String name) {
    return Attrib.events(node, name);
  }

  public TTimeSeriesCounter timeSeries(String name) {
    return Attrib.timeSeries(node, name);
  }

  public static TRuntimeProfileNode copyNode(TRuntimeProfileNode source) {
    TRuntimeProfileNode node = new TRuntimeProfileNode();
    node.setName(source.name);
    node.setCounters(new ArrayList<TCounter>());
    return node;
  }
}
