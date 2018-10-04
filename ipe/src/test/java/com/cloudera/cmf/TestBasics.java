package com.cloudera.cmf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.cloudera.cmf.LogReader.QueryRecord;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;

public class TestBasics {

  @Test
  public void testFile() throws IOException {
    File input = new File("/home/progers/data/189495", "impala_profiles_default_2.log");
//    InputStream is = new FileInputStream(input);
//    is = new BufferedInputStream(is);
//    is = new Base64InputStream(is);
//    is = new InflaterInputStream(is);
    LogReader lr = new LogReader(input);
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
  public void test() throws IOException {
    File input = new File("/home/progers/data/189495", "impala_profiles_default_1.log");
    LogReader lr = new LogReader(input);
    QueryRecord qr = lr.next();
//    TRuntimeProfileTree thriftProfile = new TRuntimeProfileTree();
//    ThriftUtil.read(input, thriftProfile, PROTOCOL_FACTORY);
//    System.out.println(thriftProfile);
    ImpalaRuntimeProfile profile = qr.profile();
    System.out.println(profile);
  }

}
