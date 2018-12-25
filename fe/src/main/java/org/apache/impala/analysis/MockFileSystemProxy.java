package org.apache.impala.analysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;

public class MockFileSystemProxy implements FileSystemProxy {

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

}
