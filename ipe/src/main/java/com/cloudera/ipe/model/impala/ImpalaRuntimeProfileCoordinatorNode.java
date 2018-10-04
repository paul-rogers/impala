// Copyright (c) 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileInstanceNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;

/**
 * A class representing the coordinator node of an impala profile. Coordinator
 * nodes are special as they imply an instance node (they have an instance id
 * that is not exposed in the profile) and sometimes contain fragment nodes for
 * certain queries, e.g., limit queries. See OPSAPS-26531 for more details.
 */
public class ImpalaRuntimeProfileCoordinatorNode
    extends ImpalaRuntimeProfileNode {

  public ImpalaRuntimeProfileCoordinatorNode(
      TRuntimeProfileNode node,
      ImpalaRuntimeProfileNode parent) {
    super(node, parent);
  }

  /**
   * From 5.11, Impala profile shows the counter in Instance node level instead of
   * the Coordinator node itself. This method checks both levels to look for the
   * counters. The assumption is that each Coordinator node has only one Instance
   * node.
   * @return Number of rows produced tracked by this coordinator.
   */
  public Long getRowsProduced() {
    TCounter counter = findCounterWithName(IPEConstants.IMPALA_PROFILE_ROWS_PRODUCED);
    if (counter == null) {
      ImpalaRuntimeProfileNode instanceNode = getInstanceNode();
      if (instanceNode != null) {
        counter = instanceNode.findCounterWithName(
            IPEConstants.IMPALA_PROFILE_ROWS_PRODUCED);
      }
    }
    return counter == null ? null : counter.getValue();
  }

  /*
   * Each Impala coordinator node should have exactly one instance node.
   */
  private ImpalaRuntimeProfileInstanceNode getInstanceNode() {
    for (ImpalaRuntimeProfileNode node : getChildren()) {
      if (node instanceof ImpalaRuntimeProfileInstanceNode) {
        return (ImpalaRuntimeProfileInstanceNode) node;
      }
    }
    return null;
  }
}
