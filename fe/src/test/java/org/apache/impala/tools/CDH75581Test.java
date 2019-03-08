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

import org.apache.commons.io.FilenameUtils;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class CDH75581Test extends FrontendTestBase {

  public String explainQuery(String query) throws ImpalaException {
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        "default", System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    queryCtx.client_request.getQuery_options().setExplain_level(TExplainLevel.EXTENDED);
    queryCtx.client_request.getQuery_options().setPlanner_testcase_mode(true);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    frontend_.createExecRequest(planCtx);
    return planCtx.getExplainString();
  }

  public void testQuery(File file) throws FileNotFoundException, IOException, ImpalaException {
    File dir = file.getParentFile();
    ProfileParser pp = new ProfileParser(file);
    String query = pp.query();
    String explainStr = explainQuery(query);

    String baseName = FilenameUtils.getBaseName(file.getName());
//    PlanAnalysisUtils.dumpLogicalPlan(planCtx.plan().logicalPlan(), new File(dir, baseName + "-logical.json"));

    File destFile = new File(dir, baseName + "-plan-I31.txt");
    QueryUtils.writeFile(destFile, explainStr);

    destFile = new File(dir, baseName + "-plan.txt");
    QueryUtils.writeFile(destFile, pp.plan());

    destFile = new File(dir, baseName + "-outline-I31.txt");
    QueryUtils.writeFile(destFile, PlanAnalysisUtils.reduce(explainStr));

    destFile = new File(dir, baseName + "-outline.txt");
    QueryUtils.writeFile(destFile, PlanAnalysisUtils.reduce(pp.plan()));
  }

  // Customer-provided file
  @Test
  public void testCDH_75581Orig() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/CDH-75581");

    CatalogBuilderListener listener = new CatalogBuilderListener(feFixture_);
    listener.cb_.createDb("saturn2");
    DumpParser parser = new DumpParser(listener);
    File dumpFile = new File(dir, "meta_sql_commands.txt");
    parser.parse(dumpFile);
    File viewFile = new File(dir, "show_view_with_explain.txt");
    parser.parse(viewFile);
//    listener.cb.dumpCatalog(new File(dir, "schema.json"));

    testQuery(new File(dir, "OLD_5.5.4_SUCCESFULL_profile_query_id_1c40bcd7cdc2a25b2942537ac52d2f91.txt"));
  }

  // Vincent's repro
  @Test
  public void testCDH_75581Repro() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/CDH-75581");

    CatalogBuilderListener listener = new CatalogBuilderListener(feFixture_);
    listener.cb_.createDb("saturn2");
    DumpParser parser = new DumpParser(listener);
    File dumpFile = new File(dir, "metadata-vincent.txt");
    listener.config().loadPartDirs = false;
    parser.parse(dumpFile);
    listener.config().disableAll();
    listener.config().loadPartDirs = true;
    parser.parse(dumpFile);
    listener.config().enableAll();
    File viewFile = new File(dir, "view-vincent.txt");
    parser.parse(viewFile);
//    listener.cb.dumpCatalog(new File(dir, "schema.json"));

    File queryFile = new File(dir, "query-vincent.txt");
    String query = Files.asCharSource(queryFile, Charsets.UTF_8).read();
    String explainStr = explainQuery(query);

    String baseName = FilenameUtils.getBaseName(queryFile.getName());

    File destFile = new File(dir, baseName + "-plan-I31.txt");
    QueryUtils.writeFile(destFile, explainStr);

    destFile = new File(dir, baseName + "-outline.txt");
    QueryUtils.writeFile(destFile, PlanAnalysisUtils.reduce(explainStr));
  }
}
