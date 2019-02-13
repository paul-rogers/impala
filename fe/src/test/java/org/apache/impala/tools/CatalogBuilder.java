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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.analysis.AlterTableSetColumnStats;
import org.apache.impala.analysis.AlterTableSetTblProperties;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CreateDbStmt;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.Parser.ParseException;
import org.apache.impala.analysis.PartitionSet;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.View;
import org.apache.impala.common.FrontendFixture;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;

import com.google.common.base.Preconditions;

public class CatalogBuilder {
  List<String> dbs_ = new ArrayList<>();
  FrontendFixture feFixture_;
  private Table mostRecentTable_;

  public CatalogBuilder(FrontendFixture feFixture) {
    feFixture_ = feFixture;
  }

  public Table mostRecentTable() { return mostRecentTable_; }
  public List<String> dbNames() { return dbs_; }
  public Catalog catalog() { return feFixture_.catalog(); }
  public Frontend frontend() { return feFixture_.frontend(); }

  private AnalysisContext makeContext() {
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryCtx().getClient_request().getQuery_options().setPlanner_testcase_mode(true);
    return ctx;
  }

  public void parseStmt(String sql) throws ImpalaException {
    StatementBase stmt = parseStmt(makeContext(), sql);
    if (stmt instanceof CreateTableStmt) {
      mostRecentTable_ = getTable(((CreateTableStmt) stmt).getTblName());
      Preconditions.checkNotNull(mostRecentTable_);
    }
  }

  public StatementBase parseStmt(AnalysisContext ctx, String sql) throws ImpalaException {
    try {
      return Parser.parse(sql, ctx.getQueryOptions());
    } catch (ParseException e) {
      System.out.println(sql);
      throw e;
    }
  }

  public void runStmt(String sql) throws ImpalaException {
    AnalysisContext ctx = makeContext();
    StatementBase parsedStmt = parseStmt(ctx, sql);
    if (parsedStmt instanceof AlterTableSetTblProperties) {
      preAnalysisCheck((AlterTableSetTblProperties) parsedStmt);
    }
    else if (parsedStmt instanceof AlterTableSetColumnStats) {
      ((AlterTableSetColumnStats) parsedStmt).setPermissiveMode();
    }
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(feFixture_.frontend(), ctx.getQueryCtx().session.database, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    AnalysisResult analysisResult = ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache, frontend().getAuthzChecker());
    StatementBase stmt = analysisResult.getStmt();
    if (stmt instanceof CreateDbStmt) {
      createDb((CreateDbStmt) stmt);
    } else if (stmt instanceof CreateTableStmt) {
      createTable(sql, (CreateTableStmt) stmt);
    } else if (stmt instanceof AlterTableSetTblProperties) {
      setTableProperties((AlterTableSetTblProperties) stmt);
    } else if (stmt instanceof AlterTableSetColumnStats) {
      setColumnStats((AlterTableSetColumnStats) stmt);
    } else if (stmt instanceof CreateViewStmt) {
      createView((CreateViewStmt) stmt);
    } else {
      throw new UnsupportedOperationException(
          "Statement not supported: " + stmt.getClass().getSimpleName());
    }
  }

  public Table getTable(TableName tableName) {
    String dbName = tableName.getDb();
    if (dbName == null || dbName.isEmpty()) return null;
    Db db = catalog().getDb(dbName);
    String baseName = tableName.getTbl();
    if (baseName == null || baseName.isEmpty()) return null;
    return db.getTable(baseName);
  }

  private void preAnalysisCheck(AlterTableSetTblProperties stmt) throws CatalogException {
    Table table = getTable(stmt.getTableName());
    if (table == null) return;
    if (!(table instanceof HdfsTable)) return;
    HdfsTable fsTable = (HdfsTable) table;
    PartitionSet partSet = stmt.getPartitionSet();
    if (partSet == null) return;
    int keyCount = fsTable.getNumClusteringCols();
    if (keyCount != partSet.getPartitionExprs().size()) return;
    String tail = "";
    List<LiteralExpr> keys = new ArrayList<>();
    for (int i = 0; i < keyCount; i++) {
      Expr expr = partSet.getPartitionExprs().get(i);
      String colName = fsTable.getColumns().get(i).getName();
      // Just let analysis fail if the expression is wrong
      if (!(expr instanceof BinaryPredicate)) return;
      Expr left = expr.getChild(0);
      if (!(left instanceof SlotRef)) return;
      Expr right = expr.getChild(1);
      if (!(right instanceof LiteralExpr)) return;
      LiteralExpr partKey = (LiteralExpr) right;
      SlotRef partName = (SlotRef) left;
      if (colName.equals(partName.getRawPath().get(0))) return;
      if (!tail.isEmpty()) tail += "/";
      tail += colName + "=" + partKey.toSql();
      keys.add(partKey);
    }
    HdfsPartition partition = makePartition(fsTable, "/" + tail, keys);
    //System.out.println(partition);
    fsTable.addPartition(partition);
  }

  HdfsPartition makePartition(HdfsTable fsTable, String suffix, List<LiteralExpr> keys) {
    HdfsPartition proto = fsTable.getPrototypePartition();
    org.apache.hadoop.hive.metastore.api.Partition msPartition = new org.apache.hadoop.hive.metastore.api.Partition();
    msPartition.setDbName(fsTable.getDb().getName());
    msPartition.setTableName(fsTable.getName());
    StorageDescriptor sd = fsTable.getMetaStoreTable().getSd();
    StorageDescriptor partSd = sd.deepCopy();
    partSd.setBucketCols(new ArrayList<>());
    partSd.setSortCols(new ArrayList<>());
    partSd.setParameters(new HashMap<>());
    partSd.setLocation(sd.getLocation() + suffix);
    msPartition.setSd(partSd);
    return new HdfsPartition(fsTable, msPartition,
        keys, proto.getInputFormatDescriptor(), new ArrayList<>(), fsTable.getAccessLevel());
  }

  private void createDb(CreateDbStmt stmt) {
    createDb(stmt.getDb(), stmt.getComment());
  }

  public void createDb(String db) {
    createDb(db, "");
  }

  public void createDb(String db, String comment) {
    dbs_.add(db);
    feFixture_.addTestDb(db, comment);
  }

  private void createTable(String sql, CreateTableStmt stmt) {
    mostRecentTable_ = feFixture_.addTestTable(sql, stmt);
  }

  private void setTableProperties(AlterTableSetTblProperties stmt) throws ImpalaException {
    // Resolution points to the catalog table, not a copy, so OK to update
    // that table directly.
    Table table = (Table) stmt.getTargetTable();
    TAlterTableParams params = stmt.toThrift();
    if (stmt.getPartitionSet() != null) {
      List<? extends FeFsPartition> partitions = stmt.getPartitionSet().getPartitions();
      HdfsPartition partition = (HdfsPartition) partitions.get(partitions.size()-1);
      Map<String, String> props = params.getSet_tbl_properties_params().getProperties();
      String value = props.get("numRows");
      if (value != null) {
        partition.setNumRows(Long.parseLong(value));
      }
      return;
    }

    switch (params.getAlter_type()) {
    case SET_TBL_PROPERTIES:
      // Put the metadata as properties, then tell the table to parse out stats
      table.getMetaStoreTable().getParameters().putAll(params.getSet_tbl_properties_params().getProperties());
      table.setTableStats(stmt.getTargetTable().getMetaStoreTable());
      break;
    default:
      throw new IllegalStateException(params.getAlter_type().name());
    }
  }

  private void setColumnStats(AlterTableSetColumnStats stmt) {
    TAlterTableParams params = stmt.toThrift();
    Table table = (Table) stmt.getTargetTable();
    ColumnStatistics colStats = CatalogOpExecutor.createHiveColStats(params.getUpdate_stats_params(), table);
    FeCatalogUtils.injectColumnStats(colStats.getStatsObj(), table);
  }

  private void createView(CreateViewStmt stmt) throws TableLoadingException {
    TCreateOrAlterViewParams params = stmt.toThrift();
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    if (catalog().containsTable(tableName.getDb(), tableName.getTbl())) {
      System.out.println(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
      return;
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    CatalogOpExecutor.setCreateViewAttributes(params, view);
    Db db = catalog().getDb(tableName.getDb());
    if (db == null)
      throw new IllegalArgumentException("DB not found: " + db);
    Table dummyTable = Table.fromMetastoreTable(db, view);
    ((View) dummyTable).localLoad();
    db.addTable(dummyTable);
  }

  public void createFiles() throws CatalogException {
    for (String dbName : dbs_) {
      Db db = catalog().getDb(dbName);
      for (String tableName : db.getAllTableNames()) {
        Table table = db.getTable(tableName);
        createTableFiles(table);
      }
    }
  }

  public void createTableFiles(Table table) throws CatalogException {
    if (!(table instanceof HdfsTable)) return;
    HdfsTable fsTable = (HdfsTable) table;
    long rowWidth = rowWidth(fsTable);
    if (rowWidth == 0) {
      double width = 0;
      for (Column col : table.getColumns()) {
        double colWidth = col.getStats().getAvgSerializedSize();
        if (colWidth == 0) {
          width = col.getStats().getAvgSize();
        }
        if (colWidth == 0) {
          width = col.getType().getColumnSize();
        }
        if (colWidth == 0) {
          colWidth = 10;
        }
        width = colWidth;
      }
      rowWidth = (int) Math.ceil(width);
      System.out.println("Estimated table width: " + table.getName() +
          ", " + rowWidth);
    }
    long rowCount = table.getTTableStats().getNum_rows();
    Collection<? extends PrunablePartition> partitions = fsTable.getPartitions();
    if (partitions.isEmpty()) {
      // Unpartitioned table. Create the dummy partition and one dummy file.
      long partSize = rowCount * rowWidth;
      HdfsPartition partition = makePartition(fsTable, "", new ArrayList<>());
      partition.setFileDescriptors(Lists.newArrayList(
          createFd(new Path(fsTable.getLocation() + "/dummy.parquet"),
              partSize)));
      partition.setNumRows(rowCount);
      fsTable.addPartition(partition);
    } else {
      // A table with partitions. Create a dummy file per partition.
      for (PrunablePartition partition : partitions) {
        HdfsPartition fsPartition = (HdfsPartition) partition;
        long partRows = fsPartition.getNumRows();
        if (partRows <= 0) {
          // Make up a row count assuming even distribution of rows
          partRows = rowCount / partitions.size();
          fsPartition.setNumRows(partRows);
          System.out.println("Estimated partition rows: " + table.getName() +
              ", partition: " + fsPartition.getId() +
              ", rows: " + partRows);
//          continue;
        }
        long partSize = partRows * rowWidth;
        fsPartition.setFileDescriptors(Lists.newArrayList(
            createFd(new Path(fsPartition.getLocation() + "/dummy.parquet"),
                partSize)));
      }
    }
  }

  public FileDescriptor createFd(Path path, long size) {
    FileStatus fileStatus = new FileStatus(size, false, 1, 0, 0, path);
    return FileDescriptor.createWithNoBlocks(fileStatus);
  }

  public long rowWidth(HdfsTable fsTable) {
    long rowCount = fsTable.getTTableStats().getNum_rows();
    long size = fsTable.getTTableStats().getTotal_file_bytes();
    if (rowCount <= 0 || size <= 0) return 0;
    return Math.round(size * 1.0D / rowCount);
  }

  public interface StmtCleaner {
    String clean(String stmt);
  }

  public void parseSql(File file) throws IOException, ImpalaException {
    parseSql(file, new StmtCleaner() {
      @Override
      public String clean(String stmt) {
        return stmt;
      }
    });
  }

  public void parseSql(File file, StmtCleaner cleaner) throws IOException, ImpalaException {
    for (String stmt : SqlFileParser.parse(file)) {
      runStmt(cleaner.clean(stmt));
    }
  }
}