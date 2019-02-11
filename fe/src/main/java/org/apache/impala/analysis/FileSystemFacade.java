package org.apache.impala.analysis;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.ListMap;

/**
 * Facade class for file system operations. Allows both a "production"
 * implementation against HDFS, and a "mock" implementation used for
 * test cases.
 */
public interface FileSystemFacade {

  /**
   * Encapsulates the block size information for a file.
   */
  public static class BlockSizeReport {
    public boolean hasBlocks_;
    public long maxBlockSize_;
  }

  /**
   * Encapsulate scan ranges for a file.
   */
  public static class ScanAllocation {
    public ExecutorMembershipSnapshot cluster = ExecutorMembershipSnapshot.getCluster();
    public Set<TNetworkAddress> localHostSet = new HashSet<>();
    public int totalNodes = 0;
    public int numLocalRanges = 0;
    public int numRemoteRanges = 0;
  }

  Path validatePath(Analyzer analyzer, Path path, Privilege privilege,
      FsAction perm) throws AnalysisException;

  BlockSizeReport maxBlockSize(Path locationPath) throws ImpalaRuntimeException;

  ScanAllocation allocateScans(FeFsTable table, TScanRangeSpec scanRangeSpecs,
      ListMap<TNetworkAddress> hostMap);
}
