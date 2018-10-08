package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;
import org.apache.impala.thrift.TUnit;

import com.google.common.base.Preconditions;

/**
 * Represents execution-time detail about an operator.
 *
 * <pre>
 * &lt;operator>_NODE (id=xx)
 *   &lt;operator>_NODE (id=xx)
 *   ...
 *   filter
 *   ...
 * </pre>
 *
 * The child operators represent the inputs to a binary
 * operator (such as a join). Subclasses exist for each
 * type of operator to provide operator-specific
 * attributes and counters.
 */
public abstract class OperatorPNode extends ProfileNode {

  /**
     * Execution information for an Exchange operator.
     */
    public static class ExchangePNode extends OperatorPNode {

      public static final String NAME_PREFIX = "EXCHANGE_NODE ";

      // Generated using EnumBuilder
      public enum Counter {
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

        private Counter(String key, TUnit units) {
          this.key = key;
          this.units = units;
        }

        public String key() { return key; }
        public TUnit units() { return units; }
      }

   // Generated using EnumBuilder
      public enum TimeSeries {
        BYTES_RECEIVED("BytesReceived", TUnit.BYTES);

        private final String key;
        private TUnit units;

        private TimeSeries(String key, TUnit units) {
          this.key = key;
          this.units = units;
        }

        public String key() { return key; }
        public TUnit units() { return units; }
      }

      public ExchangePNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
        super(analyzer, index);
      }

      @Override
      public PNodeType nodeType() { return PNodeType.EXCHANGE_OP; }

      public long counter(Counter counter) {
        return counter(counter.key());
      }

      public TTimeSeriesCounter timeSeries(TimeSeries counter) {
        return timeSeries(counter.key());
      }
    }

  /**
   * Facade for AGGREGATION nodes, both the STREAMING and FINALIZE
   * variants. The two types have distinct counters, they their
   * execution nodes have the same name. The plan indicates which
   * type this node represents (match op the operator IDs).
   */
  public static class AggPNode extends OperatorPNode {

    public static final String NAME_PREFIX = "AGGREGATION_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum StreamingCounter {
      GET_NEW_BLOCK_TIME("GetNewBlockTime", TUnit.TIME_NS),
      GET_RESULTS_TIME("GetResultsTime", TUnit.TIME_NS),
      HTRESIZE_TIME("HTResizeTime", TUnit.TIME_NS),
      HASH_BUCKETS("HashBuckets", TUnit.UNIT),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      LARGEST_PARTITION_PERCENT("LargestPartitionPercent", TUnit.UNIT),
      PARTITIONS_CREATED("PartitionsCreated", TUnit.UNIT),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PIN_TIME("PinTime", TUnit.TIME_NS),
      REDUCTION_FACTOR_ESTIMATE("ReductionFactorEstimate", TUnit.DOUBLE_VALUE),
      REDUCTION_FACTOR_THRESHOLD_TO_EXPAND("ReductionFactorThresholdToExpand", TUnit.DOUBLE_VALUE),
      ROWS_PASSED_THROUGH("RowsPassedThrough", TUnit.UNIT),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      STREAMING_TIME("StreamingTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS),
      UNPIN_TIME("UnpinTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private StreamingCounter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum FinalizeCounter {
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

      private FinalizeCounter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public AggPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.AGG_OP; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(FinalizeCounter counter) {
      return counter(counter.key());
    }

    public long counter(StreamingCounter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Facade for a Hash Join operator node. Contains two children,
   * one for each side of the join.
   * TODO: Which is the build and which is he probe side?
   */
  public static class HashJoinPNode extends OperatorPNode {

    public static final String NAME_PREFIX = "HASH_JOIN_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
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

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public HashJoinPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.HASH_JOIN_OP; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Facade for the HDFS Scan operator.
   */
  public static class HdfsScanPNode extends OperatorPNode {

    public static final String NAME_PREFIX = "HDFS_SCAN_NODE ";

    // Generated using EnumBuilder
    public enum Attrib {
      EXEC_OPTION("ExecOption"),
      HDFS_SPLIT_STATS("Hdfs split stats (<volume id>:<# splits>/<split lengths>)"),
      RUNTIME_FILTERS("Runtime filters"),
      HDFS_READ_THREAD_CONCURRENCY_BUCKET("Hdfs Read Thread Concurrency Bucket"),
      FILE_FORMATS("File Formats");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_HDFS_READ_THREAD_CONCURRENCY("AverageHdfsReadThreadConcurrency", TUnit.DOUBLE_VALUE),
      AVERAGE_SCANNER_THREAD_CONCURRENCY("AverageScannerThreadConcurrency", TUnit.DOUBLE_VALUE),
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
      PER_READ_THREAD_RAW_HDFS_THROUGHPUT("PerReadThreadRawHdfsThroughput", TUnit.BYTES_PER_SECOND),
      REMOTE_SCAN_RANGES("RemoteScanRanges", TUnit.UNIT),
      ROWS_READ("RowsRead", TUnit.UNIT),
      ROWS_RETURNED("RowsReturned", TUnit.UNIT),
      ROWS_RETURNED_RATE("RowsReturnedRate", TUnit.UNIT_PER_SECOND),
      SCAN_RANGES_COMPLETE("ScanRangesComplete", TUnit.UNIT),
      SCANNER_THREADS_INVOLUNTARY_CONTEXT_SWITCHES("ScannerThreadsInvoluntaryContextSwitches", TUnit.UNIT),
      SCANNER_THREADS_SYS_TIME("ScannerThreadsSysTime", TUnit.TIME_NS),
      SCANNER_THREADS_TOTAL_WALL_CLOCK_TIME("ScannerThreadsTotalWallClockTime", TUnit.TIME_NS),
      SCANNER_THREADS_USER_TIME("ScannerThreadsUserTime", TUnit.TIME_NS),
      SCANNER_THREADS_VOLUNTARY_CONTEXT_SWITCHES("ScannerThreadsVoluntaryContextSwitches", TUnit.UNIT),
      TOTAL_RAW_HDFS_READ_TIME("TotalRawHdfsReadTime(*)", TUnit.TIME_NS),
      TOTAL_READ_THROUGHPUT("TotalReadThroughput", TUnit.BYTES_PER_SECOND),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      BYTES_READ("BytesRead", TUnit.BYTES);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public HdfsScanPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.HDFS_SCAN_OP; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  /**
   * Detailed metrics for the code generation function of an operator.
   */
  public static class CodeGenPNode extends ProfileNode {

    public static final String NAME = "CodeGen";

    // Generated using EnumBuilder
    public enum Counter {
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

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public CodeGenPNode(ProfileFacade analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.CODE_GEN; }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Details for the data stream sender at the root of each non-root
   * fragment.
   */
  public static class DataStreamSenderPNode extends ProfileNode {

    // Generated using EnumBuilder
    public enum Counter {
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

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }
    public static final String NAME_PREFIX = "DataStreamSender ";

    public DataStreamSenderPNode(ProfileFacade analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.SENDER; }

    @Override
    public String genericName() { return NAME_PREFIX.trim(); }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Execution details for a filter. (Associated only with scan
   * nodes?)
   */
  public static class FilterPNode extends ProfileNode {

    public static String NAME_PREFIX = "Filter ";

    // Generated using EnumBuilder
    public enum Counter {
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      ROWS_PROCESSED("Rows processed", TUnit.UNIT),
      ROWS_REJECTED("Rows rejected", TUnit.UNIT),
      ROWS_TOTAL("Rows total", TUnit.UNIT),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public FilterPNode(ProfileFacade analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.FILTER; }

    @Override
    public String genericName() { return NAME_PREFIX.trim(); }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Execution details for the Block Manager functionality
   * associated with each operator.
   */
  public static class BlockMgrPNode extends ProfileNode {

    public static final Object NAME = "BlockMgr";

    // Generated using EnumBuilder
    public enum Counter {
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

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public BlockMgrPNode(ProfileFacade analyzer, int index) {
      super(analyzer, index);
    }

    @Override
    public PNodeType nodeType() { return PNodeType.BLOCK_MGR; }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  private final String operatorName;
  private final int operatorIndex;
  private List<OperatorPNode> children = new ArrayList<>();
  private final List<OperatorPNode.FilterPNode> filters = new ArrayList<>();

  public OperatorPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
    super(analyzer, index.index++);
    Pattern p = Pattern.compile("(.*)_NODE \\(id=(\\d+)\\)");
    Matcher m = p.matcher(name());
    Preconditions.checkState(m.matches());
    operatorName = m.group(1);
    operatorIndex = Integer.parseInt(m.group(2));
    for (int i = 0; i < childCount(); i++) {
      TRuntimeProfileNode profileNode = analyzer.node(index.index);
      String name = profileNode.getName();
      PNodeType nodeType = PNodeType.parse(name);
      if (nodeType == PNodeType.FILTER) {
        filters.add(new OperatorPNode.FilterPNode(analyzer, index.index++));
      } else {
        children.add(parseOperator(analyzer, name, nodeType, index));
      }
    }
  }

  public static OperatorPNode parseOperator(
      ProfileFacade analyzer, String name, PNodeType nodeType, ProfileNode.NodeIndex index) {
    switch (nodeType) {
    case EXCHANGE_OP:
      return new OperatorPNode.ExchangePNode(analyzer, index);
    case AGG_OP:
      return new OperatorPNode.AggPNode(analyzer, index);
    case HASH_JOIN_OP:
      return new OperatorPNode.HashJoinPNode(analyzer, index);
    case HDFS_SCAN_OP:
      return new OperatorPNode.HdfsScanPNode(analyzer, index);
    default:
      throw new IllegalStateException("Operator type: " + name);
    }
  }

  public int operatorId() { return operatorIndex; }

  public List<OperatorPNode> children() {
    return children;
  }

  @Override
  public String genericName() { return operatorName; }

  @Override
  public List<ProfileNode> childNodes() {
    List<ProfileNode> children = new ArrayList<>();
    children.addAll(children);
    children.addAll(filters);
    return children;
  }
}