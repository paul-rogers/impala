package org.apache.impala.analysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.FsPermissionChecker;
import org.apache.impala.util.ListMap;

/**
 * File system proxy implementation for a concrete HDFS file system.
 */
public class HdfsFileSystemFacade implements FileSystemFacade {
  private static final Configuration CONF = new Configuration();

  @Override
  public Path validatePath(Analyzer analyzer, Path path, Privilege privilege,
      FsAction perm) throws AnalysisException {
    path = FileSystemUtil.createFullyQualifiedPath(path);

    // Check if parent path exists and if impala is allowed to access it.
    Path parentPath = path.getParent();
    try {
      FileSystem fs = path.getFileSystem(FileSystemUtil.getConfiguration());
      boolean pathExists = false;
      StringBuilder errorMsg = new StringBuilder();
      try {
        pathExists = fs.exists(parentPath);
        if (!pathExists) errorMsg.append("Path does not exist.");
      } catch (Exception e) {
        errorMsg.append(e.getMessage());
      }
      if (!pathExists) {
        analyzer.addWarning(String.format("Path '%s' cannot be reached: %s",
            parentPath, errorMsg.toString()));
      } else if (perm != FsAction.NONE) {
        FsPermissionChecker checker = FsPermissionChecker.getInstance();
        if (!checker.getPermissions(fs, parentPath).checkPermissions(perm)) {
          analyzer.addWarning(String.format(
              "Impala does not have %s access to path '%s'",
              perm.toString(), parentPath));
        }
      }
    } catch (IOException e) {
      throw new AnalysisException(e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      Exception ex = e;
      if (e.getCause() != e) ex = e;
      // HDFS client's way of saying the host is unknown
      throw new AnalysisException(
          String.format("Failed to resolve HDFS URI '%s': %s",
              path, ex.getMessage()), ex);
    }
    return path;
  }

  @Override
  public BlockSizeReport maxBlockSize(Path locationPath) throws ImpalaRuntimeException {
    FileSystem partitionFs;
    try {
      partitionFs = locationPath.getFileSystem(CONF);
    } catch (IOException e) {
      throw new ImpalaRuntimeException("Error determining partition fs type", e);
    }
    BlockSizeReport report = new BlockSizeReport();
    report.hasBlocks_ = FileSystemUtil.supportsStorageIds(partitionFs);
    if (report.hasBlocks_) {
      report.maxBlockSize_ = partitionFs.getDefaultBlockSize(locationPath);
    }
    report.maxBlockSize_  = Math.max(report.maxBlockSize_,
        FileDescriptor.MIN_SYNTHETIC_BLOCK_SIZE);
    return report;
  }

  @Override
  public ScanAllocation allocateScans(FeFsTable table, TScanRangeSpec scanRangeSpecs,
      ListMap<TNetworkAddress> hostMap) {
    ScanAllocation alloc = new ScanAllocation();
    for (TScanRangeLocationList range : scanRangeSpecs.concrete_ranges) {
      boolean anyLocal = false;
      if (range.isSetLocations()) {
        for (TScanRangeLocation loc : range.locations) {
          TNetworkAddress dataNode = hostMap.getEntry(loc.getHost_idx());
          if (alloc.cluster.contains(dataNode)) {
            anyLocal = true;
            // Use the full datanode address (including port) to account for the test
            // minicluster where there are multiple datanodes and impalads on a single
            // host.  This assumes that when an impalad is colocated with a datanode,
            // there are the same number of impalads as datanodes on this host in this
            // cluster.
            alloc.localHostSet.add(dataNode);
          }
        }
      }
      // This range has at least one replica with a colocated impalad, so assume it
      // will be scheduled on one of those nodes.
      if (anyLocal) {
        ++alloc.numLocalRanges;
      } else {
        ++alloc.numRemoteRanges;
      }
      // Approximate the number of nodes that will execute locally assigned ranges to
      // be the smaller of the number of locally assigned ranges and the number of
      // hosts that hold block replica for those ranges.
      int numLocalNodes = Math.min(alloc.numLocalRanges, alloc.localHostSet.size());
      // The remote ranges are round-robined across all the impalads.
      int numRemoteNodes = Math.min(alloc.numRemoteRanges, alloc.cluster.numExecutors());
      // The local and remote assignments may overlap, but we don't know by how much
      // so conservatively assume no overlap.
      alloc.totalNodes = Math.min(numLocalNodes + numRemoteNodes, alloc.cluster.numExecutors());
      // Exit early if all hosts have a scan range assignment, to avoid extraneous
      // work in case the number of scan ranges dominates the number of nodes.
      if (alloc.totalNodes == alloc.cluster.numExecutors()) break;
    }
    return alloc;
  }
}

