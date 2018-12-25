package org.apache.impala.catalog.facade;

import java.util.List;

import org.apache.impala.analysis.LiteralExpr;

public interface PartitionFacade {
  int index();
  List<LiteralExpr> keys();
  String directoryName();
  String path();
  int rowCount();
  long sizeBytes();
  int fileCount();
}
