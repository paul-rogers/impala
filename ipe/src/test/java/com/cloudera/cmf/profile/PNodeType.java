package com.cloudera.cmf.profile;

import com.cloudera.cmf.profile.OperatorPNode.OperType;

public enum PNodeType {

  ROOT("Query "),
  SUMMARY("Summary"),
  SERVER("ImpalaServer"),
  EXEC("Execution Profile "),
  COORD("Coordinator Fragment "),
  FRAG_AVERAGE("Averaged Fragment "),
  FRAG_DETAIL("Fragment "),
  FRAG_INSTANCE("Instance "),
  EXCHANGE_OP("EXCHANGE_NODE "),
  AGG_OP("AGGREGATION_NODE "),
  HASH_JOIN_OP("HASH_JOIN_NODE "),
  HDFS_SCAN_OP("HDFS_SCAN_NODE "),
  BLOCK_MGR("BlockMgr"),
  CODE_GEN("CodeGen"),
  SENDER("DataStreamSender "),
  FILTER("Filter "),
  // Version 2.9.0 and later
  FRAG_TIMING("Fragment Instance Lifecycle Timings"),
  PLAN_ROOT("PLAN_ROOT_SINK");

  private final String namePrefix;

  private PNodeType(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  public static PNodeType parse(String name) {
    for (PNodeType nodeType : values()) {
      if (name.startsWith(nodeType.namePrefix)) {
        return nodeType;
      }
    }
    throw new IllegalStateException("Node type: " + name);
  }

  public boolean isOperator() {
    return operType() != null;
  }

  public OperType operType() {
    switch (this) {
    case PLAN_ROOT:
      return OperType.ROOT;
    case EXCHANGE_OP:
      return OperType.EXCHANGE;
    case AGG_OP:
      return OperType.AGG;
    case HASH_JOIN_OP:
      return OperType.HASH_JOIN;
    case HDFS_SCAN_OP:
      return OperType.HDFS_SCAN;
    default:
      return null;
    }
  }
}
