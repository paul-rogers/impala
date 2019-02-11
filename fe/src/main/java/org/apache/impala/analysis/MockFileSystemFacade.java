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
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.ListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockFileSystemFacade implements FileSystemFacade {
  private final static Logger LOG = LoggerFactory.getLogger(MockFileSystemFacade.class);

  @Override
  public Path validatePath(Analyzer analyzer, Path path, Privilege privilege,
      FsAction perm) throws AnalysisException {
    return path;
  }

  @Override
  public BlockSizeReport maxBlockSize(Path locationPath) throws ImpalaRuntimeException {
    BlockSizeReport report = new BlockSizeReport();
    report.hasBlocks_ = true;
    report.maxBlockSize_ = 512 * 1024 * 1024;
    return report;
  }

  @Override
  public ScanAllocation allocateScans(FeFsTable table, TScanRangeSpec scanRangeSpecs, ListMap<TNetworkAddress> hostMap) {
    ScanAllocation alloc = new ScanAllocation();
    // Track the number of unique host indexes across all scan ranges. Assume for
    // the sake of simplicity that every scan is served from a local datanode.
    Set<Integer> dummyHostIndex = new HashSet<>();
    for (TScanRangeLocationList range : scanRangeSpecs.concrete_ranges) {
      for (TScanRangeLocation loc: range.locations) {
        dummyHostIndex.add(loc.getHost_idx());
        ++alloc.numLocalRanges;
      }
    }
    int totalNodes = Math.min(
        scanRangeSpecs.concrete_ranges.size(), dummyHostIndex.size());
    LOG.info(String.format("Planner running in DEBUG mode. ScanNode: %s, " +
        "TotalNodes %d, Local Ranges %d", table.getFullName(), totalNodes,
        alloc.numLocalRanges));
    return alloc;
  }
}
