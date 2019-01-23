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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
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
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.tools.CatalogBuilder.StmtCleaner;
import org.apache.impala.tools.DumpParser.ColumnStats;
import org.apache.impala.tools.DumpParser.DirectoryDetails;
import org.apache.impala.tools.DumpParser.DumpListener;
import org.apache.impala.tools.DumpParser.PartitionDetails;
import org.apache.impala.tools.DumpParser.PartitionDump;
import org.apache.impala.tools.PlanAnalysisUtils.CardinalityModel;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MockPlanner extends FrontendTestBase {

  @Test
	public void testDump() throws IOException, ImpalaException {
    List<String> dbNames = Lists.newArrayList(
        "retrdmp01_business_users_enc",
        "retrdmp01_data_lake_enc");
    for (String dbName : dbNames) {
      System.out.println("CREATE DATABASE " + dbName);
  		addTestDb(dbName, null);
    }
		File schemaFile = new File("/home/progers/data/Prudential/export-ddl.txt");
		List<String> stmts = DumpParser.parseDump(schemaFile);
		for (String stmt : stmts) {
		  System.out.println(stmt);
		  addTestTable(stmt);
		}
	}

  @Test
  public void testSql() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    CatalogBuilder cb = new CatalogBuilder(this);
    StmtCleaner cleaner = new StmtCleaner() {

      @Override
      public String clean(String stmt) {
        stmt = ParseUtils.sanitize(stmt);
        stmt = stmt.replaceAll("\n--", "\n");
        stmt = stmt.replaceAll("'(\\d+),", "'$1',");
        return stmt;
      }
    };
    cb.parseSql(new File(dir, "pru-ddl.sql"), cleaner);
    cb.parseSql(new File(dir, "export-stats.ddl"), cleaner);
    cb.parseSql(new File(dir, "import-stats.sql"), cleaner);

    cb.createFiles();
    cb.dumpCatalog(new File(dir, "schema.json"));

    testQuery(new File(dir, "old_profile1.txt"));
    testQuery(new File(dir, "old_profile2.txt"));
  }

  public void testQuery(File file) throws FileNotFoundException, IOException, ImpalaException {
    File dir = file.getParentFile();
    ProfileParser pp = new ProfileParser(file);
    String query = pp.query();
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    queryCtx.client_request.getQuery_options().setExplain_level(TExplainLevel.EXTENDED);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    TExecRequest execRequest = frontend_.createExecRequest(planCtx);
    String explainStr = planCtx.getExplainString();

    String baseName = FilenameUtils.getBaseName(file.getName());
    PlanAnalysisUtils.dumpLogicalPlan(planCtx.plan().logicalPlan(), new File(dir, baseName + "-logical.json"));

    File destFile = new File(dir, baseName + "-plan-I31.txt");
    PrintUtils.writeFile(destFile, explainStr);

    destFile = new File(dir, baseName + "-plan.txt");
    PrintUtils.writeFile(destFile, pp.plan());

    destFile = new File(dir, baseName + "-outline-I31.txt");
    PrintUtils.writeFile(destFile, PlanAnalysisUtils.reduce(explainStr));

//    destFile = new File(dir, baseName + "-outline.txt");
//    PrintUtils.writeFile(destFile, PlanAnalysisUtils.reduce(pp.plan()));
  }

  @Test
  public void testProfileParser() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    File file = new File(dir, "old_profile1.txt");
    ProfileParser pp = new ProfileParser(file);
    System.out.println(file.getAbsolutePath());
    System.out.println("---- Query -----");
    System.out.print(pp.query());
    System.out.println("---- Plan -----");
    System.out.print(pp.plan());
    System.out.println("---- Summary -----");
    System.out.print(pp.summary());
    System.out.println("----");
  }

  @Test
  public void testReduce() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    File file = new File(dir, "old_profile1.txt");
    ProfileParser pp = new ProfileParser(file);
    System.out.println(PlanAnalysisUtils.reduce(pp.plan()));
  }

  @Test
  public void testReformat() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    File file = new File(dir, "old_profile1.txt");
    ProfileParser pp = new ProfileParser(file);
    System.out.println(PlanAnalysisUtils.cardinalityErrorTable(pp.summary()));
    file = new File(dir, "profile1.txt");
    pp = new ProfileParser(file);
    System.out.println(PlanAnalysisUtils.cardinalityErrorTable(pp.summary()));
  }

  @Test
  public void testCDH_75581Cardinality() throws FileNotFoundException, IOException {
    File dir = new File("/home/progers/data/CDH-75581");

    File goodProfile = new File(dir, "OLD_5.5.4_SUCCESFULL_profile_query_id_1c40bcd7cdc2a25b2942537ac52d2f91.txt");
    ProfileParser pp = new ProfileParser(goodProfile);
    CardinalityModel model = new CardinalityModel(pp);
    System.out.println("5.5.4 Result (Good)");
    System.out.println("Basic model");
    System.out.println(model.toString());
    System.out.println("Error model");
    System.out.println(PlanAnalysisUtils.cardinalityErrorTable(pp.summary()));
    System.out.println();

    File badProfile = new File(dir, "NEW_5.12.1_UNSUCCESFUL_profile_query_id_2b4bc11cf2f4b4a9b548dd3800000000.txt");
    pp = new ProfileParser(badProfile);
    model = new CardinalityModel(pp);
    System.out.println("5.12.1 Result (Bad)");
    System.out.println("Basic model");
    System.out.println(model.toString());
    System.out.println("Error model");
    System.out.println(PlanAnalysisUtils.cardinalityErrorTable(pp.summary()));
  }

  public static class DumpTestListener implements DumpListener {
    @Override
    public TableName stmt(String stmt) {
      System.out.println("Stmt: " + stmt.substring(0, 40) + "...");
      return null;
    }

    @Override
    public void partitionStats(TableName table, List<PartitionDump> partitions) {
      System.out.println("Partitions: " + partitions.size());
    }

    @Override
    public void partitionDirs(PartitionDetails details) {
      System.out.println("Partition: " + details.partitionName_ +
          ", entries: " + details.dirs_.size());
    }

    @Override
    public void columnStats(TableName table, List<ColumnStats> cols) {
      System.out.println("Columns: " + cols.size());
    }
  }

  @Test
  public void testDumpParser() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/CDH-75581");

    DumpTestListener listener = new DumpTestListener();
    DumpParser parser = new DumpParser(listener);
    File dumpFile = new File(dir, "meta_sql_commands.txt");
    parser.parse(dumpFile);
    File viewFile = new File(dir, "show_view_with_explain.txt");
    parser.parse(viewFile);
  }

  public static class CatalogBuilderListener implements DumpListener {
    CatalogBuilder cb;

    public CatalogBuilderListener(FrontendTestBase test) {
      cb = new CatalogBuilder(test);
    }

    @Override
    public TableName stmt(String stmt) throws ImpalaException {
      cb.runStmt(stmt);
      return cb.mostRecentTable().getTableName();
    }

    @Override
    public void partitionStats(TableName tableName, List<PartitionDump> partitions) throws ImpalaException {
//      String template = "alter table %s partition (%s=%s) set tblproperties('numRows'='%d')";
//      String sql = String.format(template, table.g)
//      System.out.println("Partitions: " + partitions.size());
      Table table = cb.getTable(tableName);
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
        HdfsPartition partition = cb.makePartition(fsTable, path, values);
        // No way to set size?
        //System.out.println(partition);
        fsTable.addPartition(partition);
      }
    }

    @Override
    public void partitionDirs(PartitionDetails details) throws ImpalaException {
      Table table = cb.getTable(details.tableName_);
      if (table == null) {
        throw new IllegalStateException();
      }
      TTableStats stats = table.getTTableStats();
      stats.setNum_rows(details.rowCount());
      stats.setTotal_file_bytes(details.sizeBytes());
      HdfsTable fsTable = (HdfsTable) table;
      Column keyCol = table.getColumns().get(0);
      for (DirectoryDetails dir : details.dirs_) {
        String key = dir.key_;
        HdfsPartition partition;
        if (key == null || key.isEmpty()) {
          if (fsTable.getPartitions().isEmpty()) {
            partition = cb.makePartition(fsTable, dir.location_, new ArrayList<>());
            fsTable.addPartition(partition);
          } else {
            partition = (HdfsPartition) fsTable.getPartitions().iterator().next();
          }
        } else {
          LiteralExpr expr = LiteralExpr.create(key, keyCol.getType());
          List<PartitionKeyValue> keyValues = Lists.newArrayList(
              new PartitionKeyValue(keyCol.getName(), expr));
          partition = fsTable.getPartition(keyValues);
        }
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
        long fileSize = dir.sizeBytes_ / dir.fileCount_;
        List<FileDescriptor> files = new ArrayList<>();
        for (int i = 0; i < dir.fileCount_; i++) {
          files.add(cb.createFd(new Path(partition.getLocation() + "/dummy " + (i+1) + ".parquet"),
              fileSize));
        }
        partition.setFileDescriptors(files);
      }
    }

    @Override
    public void columnStats(TableName tableName, List<ColumnStats> cols) {
      Table table = cb.getTable(tableName);
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

  @Test
  public void testCDH_75581() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/CDH-75581");

    CatalogBuilderListener listener = new CatalogBuilderListener(this);
    listener.cb.createDb("saturn2");
    DumpParser parser = new DumpParser(listener);
    File dumpFile = new File(dir, "meta_sql_commands.txt");
    parser.parse(dumpFile);
    File viewFile = new File(dir, "show_view_with_explain.txt");
    parser.parse(viewFile);
    listener.cb.dumpCatalog(new File(dir, "schema.json"));

    testQuery(new File(dir, "OLD_5.5.4_SUCCESFULL_profile_query_id_1c40bcd7cdc2a25b2942537ac52d2f91.txt"));
  }
}
