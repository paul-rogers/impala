// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.util;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TUnit;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileCoordinatorNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileFragmentNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileInstanceNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utility methods for parsing Impala runtime profiles.
 */
public class ImpalaRuntimeProfileUtils {

  private final static Logger LOG =
      LoggerFactory.getLogger(ImpalaRuntimeProfileUtils.class);

  /**
   * The impala TRuntimeProfileTree thrift object has all the nodes in a
   * pre-order traversal list which isn't ideal for navigation. This converts
   * it to an actual tree structure.
   *
   * This will throw an ImpalaCorruptProfileException if there are no nodes in
   * the tree or if the numChildren field is corrupt.
   * @param tree
   * @return
   */
  public static ImpalaRuntimeProfileTree convertThriftProfileToTree(
      TRuntimeProfileTree tree) {
    List<TRuntimeProfileNode> nodes = tree.getNodes();
    if (nodes.isEmpty()) {
      // If there are no nodes we can't even get the queryId so let's just
      // give up
      throw new ImpalaCorruptProfileException("No nodes in the tree");
    }
    ImpalaRuntimeProfileNode root = buildTreeHelper(new MutableInt(0), nodes, null);
    return new ImpalaRuntimeProfileTree(root, tree);
  }

  private static ImpalaRuntimeProfileNode buildTreeHelper(
      final MutableInt currentIndex,
      final List<TRuntimeProfileNode> nodes,
      final ImpalaRuntimeProfileNode parent) {
    if (currentIndex.intValue() >= nodes.size()) {
      throw new ImpalaCorruptProfileException("Wrong num children value");
    }
    TRuntimeProfileNode thriftNode = nodes.get(currentIndex.intValue());
    ImpalaRuntimeProfileNode node;
    if (thriftNode.getName() == null) {
      throw new ImpalaCorruptProfileException("Node has no name");
    }
    if (thriftNode.getName().startsWith(
        IPEConstants.IMPALA_PROFILE_FRAGMENT_PREFIX)) {
      node = new ImpalaRuntimeProfileFragmentNode(thriftNode, parent);
    } else if (thriftNode.getName().startsWith(
        IPEConstants.IMPALA_PROFILE_INSTANCE_PREFIX)) {
      node = new ImpalaRuntimeProfileInstanceNode(thriftNode, parent);
    } else if (thriftNode.getName().startsWith(
        IPEConstants.IMPALA_PROFILE_COORDINATOR_FRAGMENT)) {
      node = new ImpalaRuntimeProfileCoordinatorNode(thriftNode, parent);
    } else {
      node = new ImpalaRuntimeProfileNode(thriftNode, parent);
    }
    currentIndex.increment();

    int numChildren = thriftNode.getNum_children();
    ImmutableList.Builder<ImpalaRuntimeProfileNode> builder =
        ImmutableList.builder();
    for (int i = 0; i < numChildren; i++) {
      ImpalaRuntimeProfileNode childNode =
          buildTreeHelper(currentIndex, nodes, node);
      builder.add(childNode);
    }
    // Note that we want to construct the node object before getting the
    // children so that we can pass the children their parent. That means that
    // we have to set the children after construction.
    node.setChildren(builder.build());
    return node;
  }

  /**
   * A helper method to decompress an Impala runtime profile.
   * It takes in a zlib compressed compact, binary-encoded TRuntimeProfileTree
   * object.
   * @param compressedData
   * @return
   */
  public static byte[] decompressProfile(byte[] compressedData) {
    try {
      InflaterInputStream in =
          new InflaterInputStream(new ByteArrayInputStream(compressedData));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(in, out);
      return out.toByteArray();
    } catch (Exception e) {
      LOG.error("Error decompressing profile", e);
      throw new ImpalaCorruptProfileException("Error decompressing the profile", e);
    }
  }


  /**
   * Internal method that compresses an uncompressed profile.It takes in a
   * compact, binary-encoded TRuntimeProfileTree thrift object.
   *
   * This is only used by tests.
   * @throws IOException
   */
  public static byte[] compressProfile(final byte[] uncompressedData)
      throws IOException {
    Preconditions.checkNotNull(uncompressedData);
    DeflaterInputStream in =
        new DeflaterInputStream(new ByteArrayInputStream(uncompressedData));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copy(in, out);
    return out.toByteArray();
  }

  /**
   * Sum the counters in the list, returns null if the list is empty.
   * @param counters
   * @return
   */
  public static Double sumCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    double total = 0;
    double[] values = getCounterValuesDouble(counters);
    for (double val : values) {
      total += val;
    }
    return total;
  }

  /**
   * Sum the counters in the list, returns null if the list is empty. Rounds any
   * non-integral counter values to the nearest long.
   */
  public static Long sumLongCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    long total = 0;
    long[] values = getCounterValuesLong(counters);
    for (long val : values) {
      total += val;
    }
    return total;
  }

  public static double getDoubleValueFromCounter(TCounter counter) {
    if (counter.getUnit().equals(TUnit.DOUBLE_VALUE)) {
      return Double.longBitsToDouble(counter.getValue());
    } else {
      return counter.getValue();
    }
  }

  public static long getLongValueFromCounter(TCounter counter) {
    if (counter.getUnit().equals(TUnit.DOUBLE_VALUE)) {
      // The caller expects a long value - round the double instead of failing
      // or discarding the data.
      return Math.round(Double.longBitsToDouble(counter.getValue()));
    } else {
      return counter.getValue();
    }
  }

  private static long[] getCounterValuesLong(List<TCounter> counters) {
    long[] arr = new long[counters.size()];
    int index = 0;
    for (TCounter counter : counters) {
      arr[index] = getLongValueFromCounter(counter);
      index++;
    }
    return arr;
  }

  private static double[] getCounterValuesDouble(List<TCounter> counters) {
    double[] arr = new double[counters.size()];
    int index = 0;
    for (TCounter counter : counters) {
      arr[index] = getDoubleValueFromCounter(counter);
      index++;
    }
    return arr;
  }

  public static Long maxLongCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    long[] values = getCounterValuesLong(counters);
    return Longs.max(values);
  }

  public static Long minLongCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    long[] values = getCounterValuesLong(counters);
    return Longs.min(values);
  }

  public static Double maxCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    double[] values = getCounterValuesDouble(counters);
    return Doubles.max(values);
  }

  public static Double minCounters(List<TCounter> counters) {
    Preconditions.checkNotNull(counters);
    if (counters.isEmpty()) {
      return null;
    }
    double[] values = getCounterValuesDouble(counters);
    return Doubles.min(values);
  }
}
