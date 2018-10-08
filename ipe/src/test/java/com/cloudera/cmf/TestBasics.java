package com.cloudera.cmf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.impala.thrift.TRuntimeProfileTree;
import org.junit.Test;

import com.cloudera.cmf.printer.PlanPrinter;
import com.cloudera.cmf.printer.ProfilePrinter;
import com.cloudera.cmf.profile.EnumBuilder;
import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.scanner.AndPredicate;
import com.cloudera.cmf.scanner.CompoundAction;
import com.cloudera.cmf.scanner.LogReader;
import com.cloudera.cmf.scanner.ProfileScanner;
import com.cloudera.cmf.scanner.LogReader.QueryRecord;
import com.cloudera.cmf.scanner.PrintStmtAction;
import com.cloudera.cmf.scanner.ProfileScanner.Action;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.ThriftNodeScanAction;
import com.cloudera.cmf.scanner.StatementStatusPredicate;
import com.cloudera.cmf.scanner.StatementTypePredicate;
import com.cloudera.cmf.scanner.ThriftNodeRules.DisabledCodeGenRule;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;

public class TestBasics {

  public static File INPUT_DIR =
      new File("/home/progers/data/189495");
  public static File INPUT_FILE =
      new File(INPUT_DIR,
          "impala_profiles_default_1.log");

  @Test
  public void testFile() throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    for (;;) {
      QueryRecord qr = lr.next();
      if (qr == null) {
        break;
      }
      System.out.println("Log Time: " + qr.timestampStr());
      System.out.println("QueryId: " + qr.queryId());
      dumpAll(qr.data());
    }
    lr.close();
  }

  private void dumpAll(InputStream is) throws IOException {
    int b;
    int i = 0;
    while ((b = is.read()) != -1) {
      System.out.print(String.format("%2x", b));
      if (i == 20) {
        System.out.println();
        i = 0;
      } else {
        System.out.print(" ");
        i++;
      }
    }
    System.out.println();
    is.close();
  }

  @Test
  public void testCM() throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    QueryRecord qr = lr.next();
    ImpalaRuntimeProfile profile = qr.profile();
    System.out.println(profile);
  }

  public void testContentToConsole(int skip) throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    lr.skip(skip);

    QueryRecord qr = lr.next();
    TRuntimeProfileTree profile = qr.thriftProfile();
    ProfilePrinter printer = new ProfilePrinter(profile);
    printer.convert();
  }

  @Test
  public void testUse() throws IOException {
    testContentToConsole(0);
  }

  @Test
  public void testSomething() throws IOException {
    testContentToConsole(1);
  }

  @Test
  public void testBigQuery() throws IOException {
    testContentToFile(51);
  }

  @Test
  public void testContent() throws IOException {
    testContentToFile(3);
  }

  public void testContentToFile(int skip) throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    lr.skip(skip);

    QueryRecord qr = lr.next();
    TRuntimeProfileTree profile = qr.thriftProfile();
    File dest = new File("/home/progers", "query.txt");
    ProfilePrinter printer = new ProfilePrinter(profile, dest);
    printer.convert();
    printer.close();
  }

  @Test
  public void genEnums() throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    lr.skip(3);

    QueryRecord qr = lr.next();
    TRuntimeProfileTree profile = qr.thriftProfile();
    ProfileFacade analyzer = new ProfileFacade(profile);
    EnumBuilder.NodeType.generateAttribs(analyzer.summary().node());
  }

  @Test
  public void testScannerBasics() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .toConsole();
    scanner.scan();
    System.out.print("Scan count: ");
    System.out.println(scanner.scanCount());
    System.out.print("Accept count: ");
    System.out.println(scanner.acceptCount());
  }

  @Test
  public void testScannerPredicate() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        ;
    scanner.scan();
    System.out.print("Scan count: ");
    System.out.println(scanner.scanCount());
    System.out.print("Accept count: ");
    System.out.println(scanner.acceptCount());
  }

  @Test
  public void testDirScannerBasics() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_DIR)
        ;
    scanner.scan();
    System.out.print("Dir count: ");
    System.out.println(scanner.dirCount());
    System.out.print("File count: ");
    System.out.println(scanner.fileCount());
    System.out.print("Profile count: ");
    System.out.println(scanner.scanCount());
    System.out.print("Accept count: ");
    System.out.println(scanner.acceptCount());
  }

  public static class PrintPlanAction implements Action {

    @Override
    public void apply(ProfileFacade profile) {
      profile.computePlanSummary();
      profile.parsePlanDetails();
      PlanPrinter printer = new PlanPrinter(profile);
      printer.toStdOut();
      printer.print();
    }
  }

  @Test
  public void testPrintStmt() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        .toConsole()
        .limit(100)
        .action(new PrintStmtAction())
        ;
    scanner.scan();
  }

  @Test
  public void testPlan() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        .limit(1)
        .skip(51)
        .toConsole()
        .action(new CompoundAction()
//            .add(new PrintStmtAction())
            .add(new PrintPlanAction()))
        ;
    scanner.scan();
  }

  public static class LoadExecAction implements Action {

    @Override
    public void apply(ProfileFacade profile) {
      profile.dag().print();
    }
  }

  @Test
  public void testExec() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        .limit(1)
        .skip(51)
        .toConsole()
        .action(new CompoundAction()
//            .add(new PrintStmtAction())
            .add(new LoadExecAction()))
        ;
    scanner.scan();
  }

  public static class BuildEnumsAction implements Action {

    @Override
    public void apply(ProfileFacade profile) {
      EnumBuilder builder = new EnumBuilder(profile);
      builder.build();
      builder.print();
    }
  }

  @Test
  public void testEnumBuilder() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        .limit(1)
        .skip(51)
        .toConsole()
        .action(new CompoundAction()
//            .add(new PrintStmtAction())
            .add(new BuildEnumsAction()))
        ;
    scanner.scan();
  }

  @Test
  public void testThriftNodeScanner() throws IOException {
    ProfileScanner scanner = new ProfileScanner()
        .scanFile(INPUT_FILE)
        .predicate(
            new AndPredicate()
              .add(StatementTypePredicate.selectOnly())
              .add(StatementStatusPredicate.completedOnly()))
        .limit(1)
        .skip(51)
        .toConsole()
        .action(new CompoundAction()
//            .add(new PrintStmtAction())
            .add(new ThriftNodeScanAction()
                .add(new DisabledCodeGenRule())))
        ;
    scanner.scan();
  }
}
