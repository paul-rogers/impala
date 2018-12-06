package org.apache.impala.analysis;

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.serialize.JsonTreeFormatter;
import org.apache.impala.common.serialize.ToJsonOptions;
import org.apache.impala.testutil.TestFileParser;
import org.apache.impala.testutil.TestFileParser.Section;
import org.apache.impala.testutil.TestFileParser.TestCase;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.testutil.TestUtils.ResultFilter;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AstTest extends FrontendTestBase {
  private static java.nio.file.Path outDir_;
  private final java.nio.file.Path testDir_ = Paths.get("functional-analysis");
  private final static boolean GENERATE_OUTPUT_FILE = true;

  @BeforeClass
  public static void setUp() throws Exception {
    String logDir = System.getenv("IMPALA_FE_TEST_LOGS_DIR");
    if (logDir == null) logDir = "/tmp";
    outDir_ = Paths.get(logDir, "AstTest");
  }

  public static class AstTestFixture {
    private FrontendTestBase test_;
    public String testFileName_;
    public String testFilePath_;
    public File outputFile_;
    public PrintWriter out_;
    public int errorCount_;

    public AstTestFixture(FrontendTestBase test,
        java.nio.file.Path testDir, String testFileName) {
      test_ = test;
      testFileName_ = testFileName + ".test";
      testFilePath_ =  testDir.resolve(testFileName_).toString();
    }

    public AstTestFixture writeOutput(java.nio.file.Path outDir) throws IOException {
      File outputDir = outDir.toFile();
      outputDir.mkdirs();
      outputFile_ = new File(outputDir, testFileName_);
      out_ = new PrintWriter(new BufferedWriter(new FileWriter(outputFile_)));
      return this;
    }

    private void runTestCase(TestCase testCase) throws AnalysisException {
      String query = testCase.getQuery();
      if (query.isEmpty()) {
        fail("Cannot plan empty query in line: " +
            testCase.getStartingLineNum());
      }

      ArrayList<String> expectedPlan = testCase.getSectionContents(Section.AST);
      boolean sectionExists = expectedPlan != null && !expectedPlan.isEmpty();
      // No expected plan was specified for section. Skip expected/actual comparison.
      if (!sectionExists) return;

      StatementBase stmt = (StatementBase) test_.AnalyzesOk(query);
      JsonTreeFormatter formatter = new JsonTreeFormatter(ToJsonOptions.full());
      stmt.serialize(formatter.root());

      String astJson = formatter.toString();
      out_.println("---- AST");
      out_.print(astJson);
      List<ResultFilter> resultFilters = new ArrayList<>();
      String planDiff = TestUtils.compareOutput(
          Lists.newArrayList(astJson.split("\n")), expectedPlan, true, resultFilters);
      if (!planDiff.isEmpty()) {
        errorCount_++;
      }
    }

    public void run() {
       TestFileParser queryFileParser = new TestFileParser(testFilePath_, new TQueryOptions());

      queryFileParser.parseFile();
      for (TestCase testCase : queryFileParser.getTestCases()) {
        out_.println(testCase.getSectionAsString(Section.QUERY, true, "\n"));
        String queryOptionsSection = testCase.getSectionAsString(
            Section.QUERYOPTIONS, true, "\n");
        if (queryOptionsSection != null && !queryOptionsSection.isEmpty()) {
          out_.println("---- QUERYOPTIONS");
          out_.println(queryOptionsSection);
        }
        try {
          runTestCase(testCase);
        } catch (AnalysisException e) {
          out_.println("---- ERROR");
          out_.println(e.getMessage());
          errorCount_++;
        }
        out_.println("====");
      }
      out_.close();
      if (errorCount_ > 0) {
        fail(String.format("%d errors, see %s", errorCount_,
            outputFile_.getAbsolutePath()));
      }
    }
  }

  public void runAstTestFile(String fileName) {
    try {
      new AstTestFixture(this, testDir_, fileName)
        .writeOutput(outDir_)
        .run();
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void test() {
    runAstTestFile("basics");
  }

}
