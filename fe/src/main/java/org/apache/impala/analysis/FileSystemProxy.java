package org.apache.impala.analysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;

public interface FileSystemProxy {

  public static class BlockSizeReport {
    public boolean hasBlocks_;
    public long maxBlockSize_;
  }

  Path validatePath(Analyzer analyzer, Path path, Privilege privilege, FsAction perm) throws AnalysisException;

  BlockSizeReport maxBlockSize(Path locationPath) throws ImpalaRuntimeException;
}
