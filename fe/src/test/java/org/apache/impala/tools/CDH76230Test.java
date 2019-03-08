package org.apache.impala.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.junit.Test;

public class CDH76230Test extends FrontendTestBase {

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

  private void createDbs(File file, CatalogBuilderListener listener) throws FileNotFoundException, IOException {
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.isEmpty()) continue;
        listener.cb_.createDb(line);
      }
    }
  }

  @Test
  public void testCDH_76230() throws IOException, ImpalaException {
    File dir = new File("/home/progers/data/CDH-76230");

    CatalogBuilderListener listener = new CatalogBuilderListener(feFixture_);
    createDbs(new File(dir, "dbs.txt"), listener);
    try {
      DumpParser parser = new DumpParser(listener);
      parser.parse(new File(dir, "export.txt"));
      parser.parse(new File(dir, "export2.txt"));
      parser.parse(new File(dir, "export3.txt"));
      parser.parse(new File(dir, "views.txt"));

//      testQuery(new File(dir, "04_12_2018_profile_select_count.txt"));
      File queryFile = new File(dir, "revised-query.txt");
      String query = QueryUtils.readFile(queryFile);
      String explainStr = explainQuery(query);

      String baseName = FilenameUtils.getBaseName(queryFile.getName());

      File destFile = new File(dir, baseName + "-plan-I31.txt");
      QueryUtils.writeFile(destFile, explainStr);

    } catch (ImpalaException e) {
      System.err.println(e.getMessage());
      throw e;
    }
  }
}
