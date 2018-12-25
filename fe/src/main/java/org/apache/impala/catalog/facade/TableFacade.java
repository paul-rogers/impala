package org.apache.impala.catalog.facade;

public interface TableFacade {
  public enum TableType {
    HDFS, KUDU, VIEW, OTHER
  }
  String name();
  TableType type();
  long rowCount();
  long sizeBytes();
  int columnCount();
  ColumnFacade column(int i);
  ColumnFacade column(String name);
  Iterable<String> columnNames();
  int[] partitionCols();
  int partitionCount();
  PartitionFacade partition(int i);
}
