package com.cloudera.cmf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.impala.thrift.TRuntimeProfileTree;
import org.junit.Test;

import com.cloudera.cmf.LogReader.QueryRecord;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;

public class TestBasics {

  public static File INPUT_FILE =
      new File("/home/progers/data/189495",
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
  public void testContent() throws IOException {
    LogReader lr = new LogReader(INPUT_FILE);
    lr.skip(3);

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
    ProfileAnalyzer analyzer = new ProfileAnalyzer(profile);
    analyzer.query().generateAttribs();
  }

  @Test
  public void testScannerBasics() throws IOException {
    LogScanner scanner = new LogScanner(INPUT_FILE);
    scanner.scan();
    System.out.print("Scan count: ");
    System.out.println(scanner.scanCount());
    System.out.print("Accept count: ");
    System.out.println(scanner.acceptCount());

  }
}
