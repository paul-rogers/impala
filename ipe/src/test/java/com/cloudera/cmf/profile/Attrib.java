package com.cloudera.cmf.profile;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;
import org.apache.impala.thrift.TUnit;

public class Attrib {

  public static interface Counter {
    String key();
    TUnit units();
  }

  /**
   * Attribute names for the <code>Summary</code> node.
   */
  public static class SummaryAttrib {
    public static String SESSION_ID = "Session ID";
    public static String SESSION_TYPE = "Session Type";
    public static String HIVE_SERVER_2_PROTOCOL_VERSION = "HiveServer2 Protocol Version";
    public static String START_TIME = "Start Time";
    public static String END_TIME = "End Time";
    public static String QUERY_TYPE = "Query Type";
    public static String QUERY_STATE = "Query State";
    public static String QUERY_STATUS = "Query Status";
    public static String IMPALA_VERSION = "Impala Version";
    public static String USER = "User";
    public static String CONNECTED_USER = "Connected User";
    public static String DELEGATED_USER = "Delegated User";
    public static String NETWORK_ADDRESS = "Network Address";
    public static String DEFAULT_DB = "Default Db";
    public static String SQL_STATEMENT = "Sql Statement";
    public static String COORDINATOR = "Coordinator";
    public static String QUERY_OPTIONS_NON_DEFAULT = "Query Options  = non default)";
    public static String DDL_TYPE = "DDL Type";
    public static String PLAN = "Plan";
    public static String ESTIMATED_PER_HOST_MEM = "Estimated Per-Host Mem";
    public static String ESTIMATED_PER_HOST_VCORES = "Estimated Per-Host VCores";
    public static String REQUEST_POOL = "Request Pool";
    public static String ADMISSION_RESULT = "Admission result";
    public static String EXEC_SUMMARY = "ExecSummary";
  }

  /**
   * Event sequences. Only the <code>Summary</code> node has event
   * sequences.
   */
  public static class EventSequence {
    public static String SESSION_TYPE = "Planner Timeline";
    public static String QUERY_TIMELINE = "Query Timeline";
  }

  /**
   * Attributes for the <code>Execution Profile</code>
   * node.
   */
  public static class ExecAttrib {
    public static String NUMBER_OF_FILTERS = "Number of filters";
    public static String FILTER_ROUTING_TABLE = "Filter routing table";
    public static String FRAGMENT_START_LATENCIES = "Fragment start latencies";
    public static String FINAL_FILTER_TABLE = "Final filter table";
    public static String PER_NODE_PEAK_MEMORY_USAGE = "Per Node Peak Memory Usage";
  }

  /**
   * Counters for the <code>Execution Profile</code>
   * node.
   */
  public enum ExecCounter implements Counter {
    FILTERS_RECEIVED("FiltersReceived", TUnit.UNIT),
    FINALIZATION_TIMER("FinalizationTimer", TUnit.TIME_NS),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private ExecCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>Fragment</code>
   * node.
   */
  public enum FragDetailsCounter implements Counter {
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private FragDetailsCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>Coordinator</code>,
   * and <code>Instance</code> nodes.
   */
  public enum FragmentCounter implements Counter {
    AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
    BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
    PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
    ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
    TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
    TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
    TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
    TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private FragmentCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Time series for the <code>Instance</code>
   * node.
   */
  public enum InstanceTimeSeries implements Counter {
    MEMORY_USAGE("MemoryUsage", TUnit.BYTES),
    THREAD_USAGE("ThreadUsage", TUnit.UNIT);

    private final String key;
    private TUnit units;

    private InstanceTimeSeries(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>Averaged Fragment</code>
   * node.
   */
  public static class AveragedFragAttrib {
    public static String SPLIT_SIZES = "split sizes";
    public static String COMPLETION_TIMES = "completion times";
    public static String EXECUTION_RATES = "execution rates";
    public static String NUM_INSTANCES = "num instances";
  }

  /**
   * Attributes for the <code>Instance</code>
   * node.
   */
  public static class FragInstanceAttrib {
    public static String HDFS_SPLIT_STATS = "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";
  }

  /**
   * Counters for the <code>DataStreamSender</code>
   * node.
   */
  public enum DataStreamSenderCounter implements Counter {
    BYTES_SENT("BytesSent", TUnit.BYTES),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    NETWORK_THROUGHPUT("NetworkThroughput(*)", TUnit.BYTES_PER_SECOND),
    OVERALL_THROUGHPUT("OverallThroughput", TUnit.BYTES_PER_SECOND),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    SERIALIZE_BATCH_TIME("SerializeBatchTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS),
    TRANSMIT_DATA_RPCTIME("TransmitDataRPCTime", TUnit.TIME_NS),
    UNCOMPRESSED_ROW_BATCH_SIZE("UncompressedRowBatchSize", TUnit.BYTES);

    private final String key;
    private TUnit units;

    private DataStreamSenderCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>Filter</code>
   * node.
   */
  public enum FilterCounter implements Counter {
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    ROWS_PROCESSED("Rows processed", TUnit.UNIT),
    ROWS_REJECTED("Rows rejected", TUnit.UNIT),
    ROWS_TOTAL("Rows total", TUnit.UNIT),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private FilterCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>BlockMgr</code>
   * node.
   */
  public enum BlockMgrCounter implements Counter {
    BLOCK_WRITES_OUTSTANDING("BlockWritesOutstanding", TUnit.UNIT),
    BLOCKS_CREATED("BlocksCreated", TUnit.UNIT),
    BLOCKS_RECYCLED("BlocksRecycled", TUnit.UNIT),
    BUFFERED_PINS("BufferedPins", TUnit.UNIT),
    BYTES_WRITTEN("BytesWritten", TUnit.BYTES),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    MAX_BLOCK_SIZE("MaxBlockSize", TUnit.BYTES),
    MEMORY_LIMIT("MemoryLimit", TUnit.BYTES),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    TOTAL_BUFFER_WAIT_TIME("TotalBufferWaitTime", TUnit.TIME_NS),
    TOTAL_ENCRYPTION_TIME("TotalEncryptionTime", TUnit.TIME_NS),
    TOTAL_INTEGRITY_CHECK_TIME("TotalIntegrityCheckTime", TUnit.TIME_NS),
    TOTAL_READ_BLOCK_TIME("TotalReadBlockTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private BlockMgrCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>CodeGen</code>
   * node.
   */
  public enum CodeGenCounter implements Counter {
    CODEGEN_TIME("CodegenTime", TUnit.TIME_NS),
    COMPILE_TIME("CompileTime", TUnit.TIME_NS),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    LOAD_TIME("LoadTime", TUnit.TIME_NS),
    MODULE_BITCODE_SIZE("ModuleBitcodeSize", TUnit.BYTES),
    NUM_FUNCTIONS("NumFunctions", TUnit.UNIT),
    NUM_INSTRUCTIONS("NumInstructions", TUnit.UNIT),
    OPTIMIZATION_TIME("OptimizationTime", TUnit.TIME_NS),
    PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private CodeGenCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Attributes common to all operator nodes.
   */
  public static class OperAttrib {
    public static final String EXEC_OPTION = "ExecOption";
  }

  /**
   * Counters for the <code>EXCHANGE_NODE</code>
   * operator node.
   */
  public enum ExchangeCounter implements Counter {
    BYTES_RECEIVED("BytesReceived", TUnit.BYTES),
    CONVERT_ROW_BATCH_TIME("ConvertRowBatchTime", TUnit.TIME_NS),
    DESERIALIZE_ROW_BATCH_TIMER("DeserializeRowBatchTimer", TUnit.TIME_NS),
    FIRST_BATCH_ARRIVAL_WAIT_TIME("FirstBatchArrivalWaitTime", TUnit.TIME_NS),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
    SENDERS_BLOCKED_TIMER("SendersBlockedTimer", TUnit.TIME_NS),
    SENDERS_BLOCKED_TOTAL_TIMER("SendersBlockedTotalTimer(*)", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private ExchangeCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Time series for the <code>EXCHANGE_NODE</code>
   * operator node.
   */
  public enum ExchageTimeSeries implements Counter {
    BYTES_RECEIVED("BytesReceived", TUnit.BYTES);

    private final String key;
    private TUnit units;

    private ExchageTimeSeries(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>AGGREGATION_NODE</code>
   * operator node when describing a <code>STREAMING</code>
   * aggregation.
   */
  public enum AggStreamingCounter implements Counter {
    GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
    GET_RESULTS_TIME("GetResultsTime", TUnit.TIME_NS),
    HTRESIZE_TIME("HTResizeTime", TUnit.TIME_NS),
    HASH_BUCKETS("HashBuckets", TUnit.UNIT),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
    PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    PIN_TIME("PinTime", TUnit.TIME_NS),
    REDUCTION_FACTOR_ESTIMATE(
        "ReductionFactorEstimate", TUnit.DOUBLE_VALUE),
    REDUCTION_FACTOR_THRESHOLD_TO_EXPAND(
        "ReductionFactorThresholdToExpand", TUnit.DOUBLE_VALUE),
    ROWS_PASSED_THROUGH("RowsPassedThrough", TUnit.UNIT),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
    STREAMING_TIME("StreamingTime", TUnit.TIME_NS),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS),
    UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private AggStreamingCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>AGGREGATION_NODE</code>
   * operator node when describing a <code>FINALIZE</code>
   * aggregation.
   */
  public enum AggFinalizeCounter implements Counter {
    BUILD_TIME("BuildTime", TUnit.TIME_NS),
    GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
    GET_RESULTS_TIME("GetResultsTime", TUnit.TIME_NS),
    HTRESIZE_TIME("HTResizeTime", TUnit.TIME_NS),
    HASH_BUCKETS("HashBuckets", TUnit.UNIT),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
    MAX_PARTITION_LEVEL("MaxPartitionLevel", TUnit.UNIT),
    NUM_REPARTITIONS("NumRepartitions", TUnit.UNIT),
    PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    PIN_TIME("PinTime", TUnit.TIME_NS),
    ROWS_REPARTITIONED("RowsRepartitioned", TUnit.UNIT),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
    SPILLED_PARTITIONS("SpilledPartitions", TUnit.UNIT),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS),
    UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private AggFinalizeCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Counters for the <code>HASH_JOIN_NODE</code>
   * operator node.
   */
  public enum HashJoinCounter implements Counter {
    BUILD_PARTITION_TIME("BuildPartitionTime", TUnit.TIME_NS),
    BUILD_ROWS("BuildRows", TUnit.UNIT),
    BUILD_ROWS_PARTITIONED("BuildRowsPartitioned", TUnit.UNIT),
    BUILD_TIME("BuildTime", TUnit.TIME_NS),
    GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
    HASH_BUCKETS("HashBuckets", TUnit.UNIT),
    HASH_COLLISIONS("HashCollisions", TUnit.UNIT),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
    LOCAL_TIME("LocalTime", TUnit.TIME_NS),
    MAX_PARTITION_LEVEL("MaxPartitionLevel", TUnit.UNIT),
    NUM_REPARTITIONS("NumRepartitions", TUnit.UNIT),
    PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    PIN_TIME("PinTime", TUnit.TIME_NS),
    PROBE_ROWS("ProbeRows", TUnit.UNIT),
    PROBE_ROWS_PARTITIONED("ProbeRowsPartitioned", TUnit.UNIT),
    PROBE_TIME("ProbeTime", TUnit.TIME_NS),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
    SPILLED_PARTITIONS("SpilledPartitions", TUnit.UNIT),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS),
    UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private HashJoinCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Attributes for the <code>HDFS_SCAN_NODE</code>
   * operator node.
   */
  public static class HdfsScanAttrib {
    public static final String HDFS_SPLIT_STATS = "Hdfs split stats  = <volume id>:<# splits>/<split lengths>)";
    public static final String RUNTIME_FILTERS = "Runtime filters";
    public static final String HDFS_READ_THREAD_CONCURRENCY_BUCKET = "Hdfs Read Thread Concurrency Bucket";
    public static final String FILE_FORMATS = "File Formats";
  }

  /**
   * Counters for the <code>HDFS_SCAN_NODE</code>
   * operator node.
   */
  public enum HdfsScanCounter implements Counter {
    AVERAGE_HDFS_READ_THREAD_CONCURRENCY(
        "AverageHdfsReadThreadConcurrency", TUnit.DOUBLE_VALUE),
    AVERAGE_SCANNER_THREAD_CONCURRENCY(
        "AverageScannerThreadConcurrency", TUnit.DOUBLE_VALUE),
    BYTES_READ("BytesRead", TUnit.BYTES),
    BYTES_READ_DATA_NODE_CACHE("BytesReadDataNodeCache", TUnit.BYTES),
    BYTES_READ_LOCAL("BytesReadLocal", TUnit.BYTES),
    BYTES_READ_REMOTE_UNEXPECTED("BytesReadRemoteUnexpected", TUnit.BYTES),
    BYTES_READ_SHORT_CIRCUIT("BytesReadShortCircuit", TUnit.BYTES),
    DECOMPRESSION_TIME("DecompressionTime", TUnit.TIME_NS),
    INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
    MATERIALIZE_TUPLE_TIME("MaterializeTupleTime(*)", TUnit.TIME_NS),
    MAX_COMPRESSED_TEXT_FILE_LENGTH("MaxCompressedTextFileLength", TUnit.BYTES),
    NUM_COLUMNS("NumColumns", TUnit.UNIT),
    NUM_DISKS_ACCESSED("NumDisksAccessed", TUnit.UNIT),
    NUM_ROW_GROUPS("NumRowGroups", TUnit.UNIT),
    NUM_SCANNER_THREADS_STARTED("NumScannerThreadsStarted", TUnit.UNIT),
    PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
    PER_READ_THREAD_RAW_HDFS_THROUGHPUT(
        "PerReadThreadRawHdfsThroughput", TUnit.BYTES_PER_SECOND),
    REMOTE_SCAN_RANGES("RemoteScanRanges", TUnit.UNIT),
    ROWS_READ("RowsRead", TUnit.UNIT),
    ROWS_RETURNED("RowsReturned", TUnit.UNIT),
    ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
    SCAN_RANGES_COMPLETE("ScanRangesComplete", TUnit.UNIT),
    SCANNER_THREADS_INVOLUNTARY_CONTEXT_SWITCHES(
        "ScannerThreadsInvoluntaryContextSwitches", TUnit.UNIT),
    SCANNER_THREADS_SYS_TIME("ScannerThreadsSysTime", TUnit.TIME_NS),
    SCANNER_THREADS_TOTAL_WALL_CLOCK_TIME(
        "ScannerThreadsTotalWallClockTime", TUnit.TIME_NS),
    SCANNER_THREADS_USER_TIME("ScannerThreadsUserTime", TUnit.TIME_NS),
    SCANNER_THREADS_VOLUNTARY_CONTEXT_SWITCHES(
        "ScannerThreadsVoluntaryContextSwitches", TUnit.UNIT),
    TOTAL_RAW_HDFS_READ_TIME("TotalRawHdfsReadTime(*)", TUnit.TIME_NS),
    TOTAL_READ_THROUGHPUT("TotalReadThroughput", TUnit.BYTES_PER_SECOND),
    TOTAL_TIME("TotalTime", TUnit.TIME_NS);

    private final String key;
    private TUnit units;

    private HdfsScanCounter(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  /**
   * Time Series for the <code>HDFS_SCAN_NODE</code>
   * operator node.
   */
  public enum HdfsScanTimeSeries implements Counter {
    BYTES_READ("BytesRead", TUnit.BYTES);

    private final String key;
    private TUnit units;

    private HdfsScanTimeSeries(String key, TUnit units) {
      this.key = key;
      this.units = units;
    }

    @Override
    public String key() { return key; }
    @Override
    public TUnit units() { return units; }
  }

  public static long counter(TRuntimeProfileNode node, Counter counter) {
    return counter(node, counter.key());
  }

  public static long counter(TRuntimeProfileNode node, String name) {
    // TODO: Cache counters in a map?
    for (TCounter counter : node.getCounters()) {
      if (counter.getName().equals(name)) {
        return counter.getValue();
      }
    }
    return 0;
  }

  public static TEventSequence events(TRuntimeProfileNode node, String name) {
    // Used in only one node, only two sequences.
    // Linear search is fine.
    for (TEventSequence event : node.getEvent_sequences()) {
      if (event.getName().equals(name)) {
        return event;
      }
    }
    return null;
  }

  public static TTimeSeriesCounter timeSeries(TRuntimeProfileNode node, String name) {
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
