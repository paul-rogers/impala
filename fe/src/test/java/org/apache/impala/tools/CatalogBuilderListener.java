// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.tools;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PartitionStatsUtil;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.FrontendFixture;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.tools.DumpParser.ColumnStats;
import org.apache.impala.tools.DumpParser.DirectoryDetails;
import org.apache.impala.tools.DumpParser.DumpConfig;
import org.apache.impala.tools.DumpParser.DumpListener;
import org.apache.impala.tools.DumpParser.PartitionDetails;
import org.apache.impala.tools.DumpParser.PartitionDump;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Listens to dump loader events, then uses the information to populate
 * a test-only in-memory catalog based on information in the dump.
 */
public class CatalogBuilderListener implements DumpListener {
  DumpConfig config_ = new DumpConfig();
  CatalogBuilder cb_;

  public CatalogBuilderListener(FrontendFixture feFixture) {
    cb_ = new CatalogBuilder(feFixture);
  }

  @Override
  public DumpConfig config() { return config_; }

  @Override
  public TableName stmt(String stmt) throws ImpalaException {
    // Must parse and analyze the statement because we use the CREATE TABLE statement
    // to know when the table changes. But' don't execute if configured not to.
    try {
      if (config().runCreate) {
        cb_.runStmt(stmt);
      } else {
        cb_.parseStmt(stmt);
      }
    }
    catch (ImpalaException e) {
      System.out.println(stmt);
      throw e;
    }
    return cb_.mostRecentTable().getTableName();
  }

  @Override
  public void partitionStats(TableName tableName, List<PartitionDump> partitions) throws ImpalaException {
//      String template = "alter table %s partition (%s=%s) set tblproperties('numRows'='%d')";
//      String sql = String.format(template, table.g)
//      System.out.println("Partitions: " + partitions.size());
    Table table = cb_.getTable(tableName);
    if (table == null) {
      throw new IllegalStateException(tableName.toString());
    }
    HdfsTable fsTable = (HdfsTable) table;
    Column keyCol = table.getColumns().get(0);
    for (PartitionDump part : partitions) {
      String key = part.key_;
      String path;
      List<LiteralExpr> values;
      if (key == null || key.isEmpty()) {
        path = "";
        values = new ArrayList<>();
      } else {
        String parts[] = key.split("=");
        LiteralExpr expr = LiteralExpr.create(parts[1], keyCol.getType());
        values = Lists.newArrayList(expr);
        path = "/" + key;
      }
      HdfsPartition partition = cb_.makePartition(fsTable, path, values);
      // No way to set size?
      //System.out.println(partition);
      fsTable.addPartition(partition);
    }
  }

  @Override
  public void partitionDirs(PartitionDetails details) throws ImpalaException {
    Table table = cb_.getTable(details.tableName_);
    Preconditions.checkNotNull(table);
    System.out.println(String.format(
        "partDirs: table=%s, rows=%s, bytes=%s, dirs=%d",
        details.tableName_,
        org.apache.impala.common.PrintUtils.printMetric(details.rowCount()),
        org.apache.impala.common.PrintUtils.printBytes(details.sizeBytes()),
        details.dirs_.size()));
    TTableStats stats = table.getTTableStats();
    stats.setNum_rows(details.rowCount());
    stats.setTotal_file_bytes(details.sizeBytes());
    HdfsTable fsTable = (HdfsTable) table;
    List<Column> keyCols = table.getClusteringColumns();
    if (keyCols.size() == 0) {
      Preconditions.checkState(details.keyCols() == null);
    } else {
      Preconditions.checkState(details.keyCols().size() == keyCols.size());
    }
    for (DirectoryDetails dir : details.dirs_) {
      HdfsPartition partition;
      if (!details.isPartitioned()) {
        if (fsTable.getPartitions().isEmpty()) {
          partition = cb_.makePartition(fsTable, dir.location_, new ArrayList<>());
          fsTable.addPartition(partition);
        } else {
          partition = (HdfsPartition) fsTable.getPartitions().iterator().next();
        }
      } else {
        List<PartitionKeyValue> keyValues = new ArrayList<>();
        List<LiteralExpr> keyExprs = new ArrayList<>();
        for (int i = 0; i < keyCols.size(); i++) {
          LiteralExpr expr = LiteralExpr.create(dir.key_.get(i), keyCols.get(i).getType());
          keyValues.add(
            new PartitionKeyValue(keyCols.get(i).getName(), expr));
          keyExprs.add(expr);
        }
        partition = fsTable.getPartition(keyValues);
        if (partition == null) {
          partition = cb_.makePartition(fsTable, dir.location_, keyExprs);
          fsTable.addPartition(partition);
//          System.out.println("Partition not found: " + keyValues);
        }
      }
      Preconditions.checkNotNull(partition);
      TPartitionStats pStats = new TPartitionStats();
      TTableStats tStats = new TTableStats();
      tStats.setNum_rows(dir.rowCount_);
      tStats.setTotal_file_bytes(dir.sizeBytes_);
      pStats.setStats(tStats);
      try {
        partition.setPartitionStatsBytes(PartitionStatsUtil.partStatsToCompressedBytes(pStats), false);
      } catch (TException e) {
        throw new IllegalStateException(e);
      }
      partition.setNumRows(dir.rowCount_);
      if (dir.fileCount_ > 0) {
        long fileSize = dir.sizeBytes_ / dir.fileCount_;
        List<FileDescriptor> files = new ArrayList<>();
        for (int i = 0; i < dir.fileCount_; i++) {
          files.add(cb_.createFd(new Path(partition.getLocation() + "/dummy " + (i+1) + ".parquet"),
              fileSize));
        }
        partition.setFileDescriptors(files);
      }
    }
  }

  @Override
  public void columnStats(TableName tableName, List<ColumnStats> cols) {
    Table table = cb_.getTable(tableName);
    if (table == null) {
      throw new IllegalStateException();
    }
    for (ColumnStats colStats : cols) {
      Column col = table.getColumn(colStats.name_);
      if (col == null) {
        throw new IllegalStateException(colStats.name_);
      }
      org.apache.impala.catalog.ColumnStats stats = col.getStats();
      stats.setNumDistinctValues(colStats.ndv_);
      stats.setNumNulls(colStats.nullCount_);
      stats.setMaxSize(colStats.maxWidth);
      stats.setAvgSize(colStats.avgWidth);
    }
  }

}