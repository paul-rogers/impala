package com.cloudera.cmf.profile;

public enum PNodeType {

  ROOT,
  SUMMARY,
  SERVER,
  EXEC,
  COORD,
  FRAG_SUMMARY,
  FRAG_DETAIL,
  FRAG_INSTANCE,
  EXCHANGE_OP,
  AGG_OP,
  HASH_JOIN_OP,
  HDFS_SCAN_OP,
  BLOCK_MGR,
  CODE_GEN,
  SENDER,
  FILTER;

  public static PNodeType parse(String name) {
    if (name.startsWith("Execution Profile ")) {
      return EXEC;
    } else if (name.startsWith("Coordinator Fragment ")) {
      return COORD;
    } else if (name.equals("BlockMgr")) {
      return BLOCK_MGR;
    } else if (name.startsWith("EXCHANGE_NODE")) {
      return EXCHANGE_OP;
    } else if (name.startsWith("Averaged Fragment ")) {
      return FRAG_SUMMARY;
    } else if (name.startsWith("Fragment ")) {
      return FRAG_DETAIL;
    } else if (name.startsWith("Instance ")) {
      return FRAG_INSTANCE;
    } else if (name.equals("CodeGen")) {
      return CODE_GEN;
    } else if (name.startsWith("DataStreamSender ")) {
      return SENDER;
    } else if (name.startsWith("AGGREGATION_NODE ")) {
      return AGG_OP;
    } else if (name.startsWith("HASH_JOIN_NODE ")) {
      return HASH_JOIN_OP;
    } else if (name.startsWith("HDFS_SCAN_NODE ")) {
      return HDFS_SCAN_OP;
    } else if (name.startsWith("Filter ")) {
      return FILTER;
    } else if (name.equals("ImpalaServer")) {
      return SERVER;
    } else if (name.equals("Summary")) {
      return SUMMARY;
    } else if (name.startsWith("Query ")) {
      return ROOT;
    } else {
      throw new IllegalStateException("Node type: " + name);
    }
  }

  public boolean isOperator() {
    switch (this) {
    case EXCHANGE_OP:
    case AGG_OP:
    case HASH_JOIN_OP:
    case HDFS_SCAN_OP:
      return true;
    default:
      return false;
    }
  }
}
