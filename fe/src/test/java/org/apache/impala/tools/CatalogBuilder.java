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
import java.io.FileWriter;
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
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.serialize.ArraySerializer;
import org.apache.impala.common.serialize.JacksonTreeSerializer;
import org.apache.impala.common.serialize.ToJsonOptions;
import org.apache.impala.service.CatalogOpExecutor.CatalogMutator;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;

import com.google.common.base.Preconditions;

public class CatalogBuilder {
  List<String> dbs_ = new ArrayList<>();
  FrontendTestBase test_;
  private Table mostRecentTable_;

  public CatalogBuilder(FrontendTestBase test) {
    test_ = test;
  }

  public Table mostRecentTable() { return mostRecentTable_; }
  public List<String> dbNames() { return dbs_; }
  public Catalog catalog() { return FrontendTestBase.catalog_; }

  public void runStmt(String sql) throws ImpalaException {
    sql = sql.replaceAll("\n--", "\n");
    sql = sql.replaceAll("'(\\d+),", "'$1',");
    AnalysisContext ctx = test_.createAnalysisCtx();
    StatementBase parsedStmt = Parser.parse(sql, ctx.getQueryOptions());
    if (parsedStmt instanceof AlterTableSetTblProperties) {
      preAnalysisCheck((AlterTableSetTblProperties) parsedStmt);
    }
    else if (parsedStmt instanceof AlterTableSetColumnStats) {
      ((AlterTableSetColumnStats) parsedStmt).setPermissiveMode();
    }
    StmtMetadataLoader mdLoader =
        new StmtMetadataLoader(FrontendTestBase.frontend_, ctx.getQueryCtx().session.database, null);
    StmtTableCache stmtTableCache = mdLoader.loadTables(parsedStmt);
    AnalysisResult analysisResult = ctx.analyzeAndAuthorize(parsedStmt, stmtTableCache, MockPlanner.frontend_.getAuthzChecker());
    StatementBase stmt = (StatementBase) analysisResult.getStmt();;
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

  private void preAnalysisCheck(AlterTableSetTblProperties stmt) throws CatalogException {
    String dbName = stmt.getTableName().getDb();
    if (dbName == null || dbName.isEmpty()) return;
    Db db = MockPlanner.catalog_.getDb(dbName);
    String tableName = stmt.getTableName().getTbl();
    if (tableName == null || tableName.isEmpty()) return;
    Table table = db.getTable(tableName);
    //System.out.println(table.toString());
    if (!(table instanceof HdfsTable)) return;
    HdfsTable fsTable = (HdfsTable) table;
    PartitionSet partSet = stmt.getPartitionSet();
    if (partSet == null) return;
    for (Expr expr : partSet.getPartitionExprs()) {
      // Just let analysis fail if the expression is wrong
      if (!(expr instanceof BinaryPredicate)) return;
      Expr left = expr.getChild(0);
      if (!(left instanceof SlotRef)) return;
      Expr right = expr.getChild(1);
      if (!(right instanceof LiteralExpr)) return;
      LiteralExpr partKey = (LiteralExpr) right;
      SlotRef partName = (SlotRef) left;
      List<String> path = partName.getRawPath();
      if (path.size() != 1) return;
      String name = path.get(0);
      //System.out.println("Create partition: " + name + "=" + partKey.toSql());
      //System.out.println("Prototype: " + proto.toString());
      HdfsPartition partition = makePartition(fsTable, "/" + name + "=" + partKey.toSql(), Lists.newArrayList(partKey));
      //System.out.println(partition);
      fsTable.addPartition(partition);
    }
  }

  private HdfsPartition makePartition(HdfsTable fsTable, String suffix, List<LiteralExpr> keys) {
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
    dbs_.add(stmt.getDb());
    test_.addTestDb(stmt.getDb(), stmt.getComment());
  }

  private void createTable(String sql, CreateTableStmt stmt) {
    mostRecentTable_ = test_.addTestTable(sql, stmt);
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
    ColumnStatistics colStats = CatalogMutator.createHiveColStats(params.getUpdate_stats_params(), table);
    FeCatalogUtils.injectColumnStats(colStats.getStatsObj(), table);
  }

  private void createView(CreateViewStmt stmt) throws TableLoadingException {
    TCreateOrAlterViewParams params = stmt.toThrift();
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    if (MockPlanner.catalog_.containsTable(tableName.getDb(), tableName.getTbl())) {
      System.out.println(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
      return;
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    CatalogMutator.setCreateViewAttributes(params, view);
    Db db = MockPlanner.catalog_.getDb(tableName.getDb());
    if (db == null)
      throw new IllegalArgumentException("DB not found: " + db);
    Table dummyTable = Table.fromMetastoreTable(db, view);
    ((View) dummyTable).localLoad();
    db.addTable(dummyTable);
  }

  public void createFiles() throws CatalogException {
    for (String dbName : dbs_) {
      Db db = MockPlanner.catalog_.getDb(dbName);
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
      // Unpartitioned table. Create the dummy parition and one dummy file.
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

  public void dumpCatalog(File destFile) throws IOException {
    try (FileWriter out = new FileWriter(destFile);
        JacksonTreeSerializer ts = new JacksonTreeSerializer(out, ToJsonOptions.full())) {
      ArraySerializer as = ts.root().array("dbs");
      for (String db : dbNames()) {
        catalog().getDb(db).serialize(as.object());
      }
    }
  }

  public void parseSql(File file) throws IOException, ImpalaException {
    List<String> stmts = SqlFileParser.parse(file);

    for (String stmt : stmts) {
      stmt = ParseUtils.sanitize(stmt);
//      System.out.println(stmt);
      runStmt(stmt);
    }
  }

}