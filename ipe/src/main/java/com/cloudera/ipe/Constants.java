package com.cloudera.ipe;

public class Constants {

  public static final String IMPALA_PROFILE_SUMMARY = "Summary";
  public static final String IMPALA_PROFILE_ROWS_PRODUCED = "RowsProduced";
  public static final String IMPALA_PROFILE_FRAGMENT_PREFIX = "Fragment ";
  public static final String IMPALA_PROFILE_INSTANCE_PREFIX = "Instance";
  public static final String IMPALA_START_TIME_INFO_STRING = "Start Time";
  public static final String IMPALA_END_TIME_INFO_STRING = "End Time";
  public static final String IMPALA_QUERY_TYPE_INFO_STRING = "Query Type";
  public static final String IMPALA_QUERY_STATE_INFO_STRING = "Query State";
  public static final String IMPALA_USER_INFO_STRING = "User";
  public static final String IMPALA_CONNECTED_USER_INFO_STRING = "Connected User";
  public static final String IMPALA_DELEGATED_USER_INFO_STRING = "Delegated User";
  public static final String IMPALA_DEFAULT_DB_INFO_STRING = "Default Db";
  public static final String IMPALA_SQL_STATEMENT_INFO_STRING = "Sql Statement";
  public static final String IMPALA_IMPALA_VERSION_INFO_STRING = "Impala Version";
  public static final String IMPALA_SESSION_ID_INFO_STRING = "Session ID";
  public static final String IMPALA_DDL_TYPE_INFO_STRING = "DDL Type";
  public static final String IMPALA_SESSION_TYPE_INFO_STRING = "Session Type";
  public static final String IMPALA_NETWORK_ADDRESS_INFO_STRING = "Network Address";
  public static final String IMPALA_QUERY_STATUS_INFO_STRING = "Query Status";
  public static final String IMPALA_QUERY_PLAN = "Plan";
  public static final String IMPALA_PROFILE_TOTAL_TIME = "TotalTime";
  public static final String IMPALA_PROFILE_TOTAL_CPU_TIME = "TotalCpuTime";
  public static final String IMPALA_PROFILE_TOTAL_SYS_TIME = "TotalThreadsSysTime";
  public static final String IMPALA_PROFILE_TOTAL_USER_TIME = "TotalThreadsUserTime";
  public static final String IMPALA_PROFILE_TOTAL_STORAGE_WAIT_TIME =
      "TotalStorageWaitTime";
  public static final String IMPALA_PROFILE_TOTAL_NETWORK_SEND_TIME =
      "TotalNetworkSendTime";
  public static final String IMPALA_PROFILE_TOTAL_NETWORK_RECEIVE_TIME =
      "TotalNetworkReceiveTime";
  public static final String IMPALA_PROFILE_CLIENT_FETCH_WAIT_TIMER = "ClientFetchWaitTimer";
  public static final String IMPALA_PROFILE_ALL_NODES_PREFIX =
      "";
  public static final String IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX =
      "HDFS_SCAN_NODE";
  public static final String IMPALA_PROFILE_HDFS_SINK_NODE_PREFIX =
      "HdfsTableSink";
  public static final String IMPALA_PROFILE_HBASE_SCAN_NODE_PREFIX =
      "HBASE_SCAN_NODE";
  public static final String IMPALA_PROFILE_DATA_STREAM_SENDER_NODE =
      "DataStreamSender";
  public static final String IMPALA_PROFILE_ROWS_INSERTED = "RowsInserted";
  public static final String IMPALA_PROFILE_BYTES_WRITTEN = "BytesWritten";
  public static final String IMPALA_PROFILE_BYTES_READ = "BytesRead";
  public static final String IMPALA_PROFILE_BYTES_READ_LOCAL = "BytesReadLocal";
  public static final String IMPALA_PROFILE_BYTES_READ_SHORT_CIRCUIT = "BytesReadShortCircuit";
  public static final String IMPALA_PROFILE_BYTES_READ_FROM_CACHE = "BytesReadDataNodeCache";
  public static final String IMPALA_PROFILE_BYTES_SKIPPED = "BytesSkipped";
  public static final String IMPALA_PROFILE_BYTES_SENT = "BytesSent";
  public static final String IMPALA_PROFILE_SCAN_RANGES = "ScanRangesComplete";
  public static final String IMPALA_PROFILE_FILE_FORMATS = "File Formats";
  public static final String IMPALA_PROFILE_RAW_HDFS_READ_TIME =
      "TotalRawHdfsReadTime(*)";
  public static final String IMPALA_PROFILE_RAW_HBASE_READ_TIME =
      "TotalRawHBaseReadTime(*)";
  public static final String IMPALA_PROFILE_COORDINATOR_FRAGMENT = "Coordinator Fragment";

  public static final String IMPALA_QUERY_STATE_CREATED = "CREATED";
  public static final String IMPALA_QUERY_STATE_COMPILED = "COMPILED";
  public static final String IMPALA_QUERY_STATE_RUNNING = "RUNNING";
  public static final String IMPALA_QUERY_STATE_FINISHED = "FINISHED";
  public static final String IMPALA_QUERY_STATE_EXCEPTION = "EXCEPTION";
  public static final String IMPALA_QUERY_STATE_INITIALIZED = "INITIALIZED";
  public static final String IMPALA_QUERY_STATE_UNKNOWN = "UNKNOWN";

  public static final String IMPALA_QUERY_TYPE_QUERY = "QUERY";
  public static final String IMPALA_QUERY_TYPE_DML = "DML";
  public static final String IMPALA_QUERY_TYPE_DDL = "DDL";
  public static final String IMPALA_QUERY_TYPE_UNKNOWN = "UNKNOWN";

  public static final String IMPALA_SESSION_TYPE_BEESWAX = "BEESWAX";
  public static final String IMPALA_SESSION_TYPE_HIVESERVER2 = "HIVESERVER2";

  public static final String IMPALA_TIME_SERIES_COUNTER_MEMORY_USAGE = "MemoryUsage";
  public static final String IMPALA_TIME_SERIES_COUNTER_THREAD_USAGE = "ThreadUsage";

  public static final String IMPALA_QUERY_ATTRIBUTE_QUERY_STATUS = "query_status";
  public static final String IMPALA_QUERY_ATTRIBUTE_VALUE_CANCELLED = "Cancelled";

  public static final String IMPALA_QUERY_ATTRIBUTE_REQUEST_POOL = "Request Pool";
  public static final String IMPALA_QUERY_ATTRIBUTE_TABLES_MISSING_STATS =
      "Tables Missing Stats";
  public static final String IMPALA_QUERY_ATTRIBUTE_TABLES_CORRUPT_STATS =
      "Tables With Corrupt Table Stats";
  public static final String IMPALA_QUERY_ATTRIBUTE_ESTIMATED_PER_HOST_MEMORY =
      "Estimated Per-Host Mem";

  public static final String IMPALA_PROFILE_EVENT_RESOURCES_RESERVED = "Resources reserved";
  public static final String IMPALA_PROFILE_EVENT_PLANNING_FINISHED = "Planning finished";
  public static final String IMPALA_PER_NODE_PEAK_MEMORY_USAGE = "Per Node Peak Memory Usage";
}
