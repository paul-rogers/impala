// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe;

import com.google.common.collect.ImmutableList;

/**
 * Configuration constants used by the impala profile extractor library. Also
 * contains default values for analysis rules, when none are set explicitly.
 */
public class IPEConstants {
  public static final String IMPALA_SESSION_TYPE_BEESWAX = "BEESWAX";
  public static final String IMPALA_SESSION_TYPE_HIVESERVER2 = "HIVESERVER2";

  public static final ImmutableList<String> DEFAULT_IMPALA_RUNTIME_PROFILE_TIME_FORMATS = ImmutableList
      .of("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS");
  public static final ImmutableList<String> DEFAULT_IMPALA_SESSION_TYPES = ImmutableList
      .of(IMPALA_SESSION_TYPE_BEESWAX, IMPALA_SESSION_TYPE_HIVESERVER2);
  public static final String IMPALA_TIMEFORMAT_PROPERTY = "impala-timeFormatters";
  public static final String IMPALA_SESSIONTYPES_PROPERTY = "impala-session-types";

  public static final long SECOND_IN_MILLI_SECONDS = 1000L;
  public static final long SECOND_IN_NANO_SECONDS = 1000 * 1000 * 1000L;

  public static final String IMPALA_CONNECTED_USER_INFO_STRING = "Connected User";
  public static final String IMPALA_DDL_TYPE_INFO_STRING = "DDL Type";
  public static final String IMPALA_DEFAULT_DB_INFO_STRING = "Default Db";
  public static final String IMPALA_DELEGATED_USER_INFO_STRING = "Delegated User";
  public static final String IMPALA_END_TIME_INFO_STRING = "End Time";
  public static final String IMPALA_IMPALA_VERSION_INFO_STRING = "Impala Version";
  public static final String IMPALA_NETWORK_ADDRESS_INFO_STRING = "Network Address";
  public static final String IMPALA_PER_NODE_PEAK_MEMORY_USAGE = "Per Node Peak Memory Usage";
  public static final String IMPALA_PROFILE_ALL_NODES_PREFIX = "";
  public static final String IMPALA_PROFILE_BYTES_READ = "BytesRead";
  public static final String IMPALA_PROFILE_BYTES_READ_FROM_CACHE = "BytesReadDataNodeCache";
  public static final String IMPALA_PROFILE_BYTES_READ_LOCAL = "BytesReadLocal";
  public static final String IMPALA_PROFILE_BYTES_READ_SHORT_CIRCUIT = "BytesReadShortCircuit";
  public static final String IMPALA_PROFILE_BYTES_SENT = "BytesSent";
  public static final String IMPALA_PROFILE_BYTES_SKIPPED = "BytesSkipped";
  public static final String IMPALA_PROFILE_BYTES_WRITTEN = "BytesWritten";
  public static final String IMPALA_PROFILE_CLIENT_FETCH_WAIT_TIMER = "ClientFetchWaitTimer";
  public static final String IMPALA_PROFILE_COORDINATOR_FRAGMENT = "Coordinator Fragment";
  public static final String IMPALA_PROFILE_DATA_STREAM_SENDER_NODE = "DataStreamSender";
  public static final String IMPALA_PROFILE_EVENT_PLANNING_FINISHED = "Planning finished";
  public static final String IMPALA_PROFILE_EVENT_RESOURCES_RESERVED = "Resources reserved";
  public static final String IMPALA_PROFILE_FILE_FORMATS = "File Formats";
  public static final String IMPALA_PROFILE_FRAGMENT_PREFIX = "Fragment ";
  public static final String IMPALA_PROFILE_HDFS_SCAN_NODE_PREFIX = "HDFS_SCAN_NODE";
  public static final String IMPALA_PROFILE_HDFS_SINK_NODE_PREFIX = "HdfsTableSink";
  public static final String IMPALA_PROFILE_HBASE_SCAN_NODE_PREFIX = "HBASE_SCAN_NODE";
  public static final String IMPALA_PROFILE_INSTANCE_PREFIX = "Instance";
  public static final String IMPALA_PROFILE_RAW_HDFS_READ_TIME = "TotalRawHdfsReadTime(*)";
  public static final String IMPALA_PROFILE_RAW_HBASE_READ_TIME = "TotalRawHBaseReadTime(*)";
  public static final String IMPALA_PROFILE_ROWS_INSERTED = "RowsInserted";
  public static final String IMPALA_PROFILE_ROWS_PRODUCED = "RowsProduced";
  public static final String IMPALA_PROFILE_SCAN_RANGES = "ScanRangesComplete";
  public static final String IMPALA_PROFILE_TOTAL_CPU_TIME = "TotalCpuTime";
  public static final String IMPALA_PROFILE_TOTAL_STORAGE_WAIT_TIME = "TotalStorageWaitTime";
  public static final String IMPALA_PROFILE_TOTAL_NETWORK_SEND_TIME = "TotalNetworkSendTime";
  public static final String IMPALA_PROFILE_TOTAL_NETWORK_RECEIVE_TIME = "TotalNetworkReceiveTime";

  public static final String IMPALA_PROFILE_TOTAL_SYS_TIME = "TotalThreadsSysTime";
  public static final String IMPALA_PROFILE_TOTAL_TIME = "TotalTime";
  public static final String IMPALA_PROFILE_TOTAL_USER_TIME = "TotalThreadsUserTime";
  public static final String IMPALA_QUERY_ATTRIBUTE_QUERY_STATUS = "query_status";
  public static final String IMPALA_QUERY_ATTRIBUTE_REQUEST_POOL = "Request Pool";
  public static final String IMPALA_QUERY_ATTRIBUTE_TABLES_MISSING_STATS = "Tables Missing Stats";
  public static final String IMPALA_QUERY_ATTRIBUTE_TABLES_CORRUPT_STATS = "Tables With Corrupt Table Stats";
  public static final String IMPALA_QUERY_ATTRIBUTE_ESTIMATED_PER_HOST_MEMORY = "Estimated Per-Host Mem";

  public static final String IMPALA_QUERY_PLAN = "Plan";
  public static final String IMPALA_QUERY_STATE_EXCEPTION = "EXCEPTION";
  public static final String IMPALA_QUERY_STATE_INFO_STRING = "Query State";
  public static final String IMPALA_QUERY_STATUS_INFO_STRING = "Query Status";
  public static final String IMPALA_QUERY_TYPE_INFO_STRING = "Query Type";
  public static final String IMPALA_QUERY_TYPE_QUERY = "QUERY";
  public static final String IMPALA_SESSION_ID_INFO_STRING = "Session ID";
  public static final String IMPALA_SESSION_TYPE_INFO_STRING = "Session Type";
  public static final String IMPALA_SQL_STATEMENT_INFO_STRING = "Sql Statement";
  public static final String IMPALA_START_TIME_INFO_STRING = "Start Time";
  public static final String IMPALA_TIME_SERIES_COUNTER_MEMORY_USAGE = "MemoryUsage";
  public static final String IMPALA_TIME_SERIES_COUNTER_THREAD_USAGE = "ThreadUsage";
  public static final String IMPALA_USER_INFO_STRING = "User";

  public static final long NANOS_PER_MILLIS = 1000000L;
  public static final long ONE_KILOBYTE = 1024L;
  public static final long ONE_MEGABYTE = 1024L * ONE_KILOBYTE;
  public static final long TEN_MEGABYTES = 10L * ONE_MEGABYTE;
  public static final long ONE_GIGABYTE = 1024L * ONE_MEGABYTE;

  public static final long FIFTY_MEGABYTES = 50L * ONE_MEGABYTE;
  public static final long TEN_GIGABYTES = 10L * ONE_GIGABYTE;
  public static final long ONE_TERABYTE = 1024L * ONE_GIGABYTE;
  public static final long TWO_HUNDRED_MEGABYTES = 200L * ONE_MEGABYTE;
  public static final long TWO_HUNDRED_FIFTY_SIX_MEGABYTES = 256L
      * ONE_MEGABYTE;
  public static final long DEFAULT_HDFS_BLOCK_SIZE = 128 * ONE_MEGABYTE;
  public static final String PLANNER_TIMELINE_INDEX = "Planner Timeline";
  public static final String QUERY_TIMELINE_INDEX = "Query Timeline";

  public static final String UNITS_BYTES = "bytes";
  public static final String UNITS_CORES = "cores";
  public static final String UNITS_SECONDS = "seconds";
  public static final String UNITS_MILLISECONDS = "ms";
  public static final String UNITS_MICROSECONDS = "micros";
  public static final String UNITS_BYTE_SECONDS = "byte seconds";
  public static final String UNITS_MEGABYTE_MILLIS = "mb millis";
  public static final String UNITS_VCORE_MILLIS = "vcore millis";
  public static final String UNITS_REQUESTS = "requests";
  public static final String UNITS_ITEMS = "items";
  public static final String UNITS_ROWS = "rows";
  public static final String UNITS_RECORDS = "records";
  public static final String UNITS_PERCENT = "percent";
  public static final String UNITS_TASKS = "tasks";
  public static final String UNITS_FAILURES = "failures";
  public static final String UNITS_ACTIVITY_ID = "activity id";
  public static final String UNITS_USER = "user";
  public static final String UNITS_ENTITIES = "entities";
  public static final String UNITS_READS = "reads";
  public static final String UNITS_WRITES = "writes";
  public static final String UNITS_METRICS = "metrics";
  public static final String UNITS_MINUTE = "minute";
  public static final String UNITS_DATAPOINTS = "datapoints";
  public static final String UNITS_PLANS = "plans";
}
