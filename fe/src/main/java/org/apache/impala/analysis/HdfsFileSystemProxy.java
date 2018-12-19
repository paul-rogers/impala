package org.apache.impala.analysis;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.util.FsPermissionChecker;

public class HdfsFileSystemProxy implements FileSystemProxy {

  @Override
  public Path validatePath(Analyzer analyzer, Path path, Privilege privilege, FsAction perm) throws AnalysisException {
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
}
