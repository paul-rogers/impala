package org.apache.impala.analysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;

public class MockFileSystemProxy implements FileSystemProxy {

  @Override
  public Path validatePath(Analyzer analyzer, Path path, Privilege privilege,
      FsAction perm) throws AnalysisException {
    return path;
  }

}
