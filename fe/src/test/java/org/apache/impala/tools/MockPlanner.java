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
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.tools.DumpParser.DumpListener;
import org.apache.impala.tools.DumpParser.PartitionDump;
import org.apache.impala.tools.PlanAnalysisUtils.CardinalityModel;
import org.junit.Test;

public class MockPlanner extends FrontendTestBase {

  @Test
	public void testDump() throws IOException {
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
  public void tesStSql() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/Prudential");
    CatalogBuilder cb = new CatalogBuilder(this);
    cb.parseSql(new File(dir, "pru-ddl.sql"));
    cb.parseSql(new File(dir, "export-stats.ddl"));
    cb.parseSql(new File(dir, "import-stats.sql"));

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
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.becomeMock();
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

    destFile = new File(dir, baseName + "-outline.txt");
    PrintUtils.writeFile(destFile, PlanAnalysisUtils.reduce(pp.plan()));
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

//  private static double parseRows(String rows) {
//    Pattern p = Pattern.compile("([0-9.]+)([KMB]?)");
//    Matcher m = p.matcher(rows);
//    if (!m.matches()) {
//      throw new IllegalStateException(rows);
//    }
//    double value = Double.parseDouble(m.group(1));
//    String unit = m.group(2);
//    switch (unit) {
//    case "":
//      return value;
//    case "K":
//      return value * 1000;
//    case "M":
//      return value * 1_000_000;
//    case "B":
//      return value * 1_000_000_000;
//    default:
//      throw new IllegalStateException(unit);
//    }
//  }

  @Test
  public void testCDH_75581() throws FileNotFoundException, IOException {
    File dir = new File("/home/progers/data/CDH-75581");

    File goodProfile = new File(dir, "OLD_5.5.4_SUCCESFULL_profile_query_id_1c40bcd7cdc2a25b2942537ac52d2f91.txt");
    ProfileParser pp = new ProfileParser(goodProfile);
    CardinalityModel model = new CardinalityModel(pp);
    System.out.println("5.5.4 Result (Good)");
    System.out.println(model.toString());

    File badProfile = new File(dir, "NEW_5.12.1_UNSUCCESFUL_profile_query_id_2b4bc11cf2f4b4a9b548dd3800000000.txt");
    pp = new ProfileParser(badProfile);
    model = new CardinalityModel(pp);
    System.out.println("5.12.1 Result (Bad)");
    System.out.println(model.toString());
  }

  public static class TestDumpTestListner implements DumpListener {
    @Override
    public String stmt(String stmt) {
      System.out.println("Stmt: " + stmt.substring(0, 40) + "...");
      return null;
    }

    @Override
    public void tableDetails(String table, List<PartitionDump> partitions) {
      System.out.println("Partitions: " + partitions.size());
    }
  }


  @Test
  public void testDumpParser() throws IOException {
    File dir = new File("/home/progers/data/CDH-75581");

    File dumpFile = new File(dir, "meta_sql_commands.txt");
    TestDumpTestListner listener = new TestDumpTestListner();
    try (DumpParser parser = new DumpParser(dumpFile, listener)) {
      parser.load();
    }
  }
}
