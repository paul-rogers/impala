package com.cloudera.cmf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TTimeSeriesCounter;
import org.junit.Test;

import com.cloudera.cmf.LogReader.QueryRecord;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;

public class TestBasics {

  @Test
  public void testFile() throws IOException {
    File input = new File("/home/progers/data/189495", "impala_profiles_default_2.log");
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
    ImpalaRuntimeProfile profile = qr.profile();
    System.out.println(profile);
  }

  public static class ProfileWalker {

    TRuntimeProfileTree profile;
    PrintWriter out;
    int indent;

    public ProfileWalker(TRuntimeProfileTree profile) {
      this.profile = profile;
      out = new PrintWriter(new OutputStreamWriter(System.out), true);
    }

    public void convert() {
      writeLabel("Query");
      indent++;
      for (TRuntimeProfileNode node : profile.getNodes()) {
        convertNode(node);
      }
      indent--;
      out.flush();
    }

    public void convertNode(TRuntimeProfileNode node) {
      writeLabel(node.getName());
      indent++;
      write("Node ID", node.getMetadata());
      Map<String, String> info = node.getInfo_strings();
      List<String> keys = node.getInfo_strings_display_order();
      if (info.size() != keys.size()) {
        writeIndent();
        out.print(String.format("WARNING: map size = %d, key size = %d",
            info.size(), keys.size()));
      }
      if (! info.isEmpty()) {
        writeLabel("Info");
        indent++;
        for (String key : keys) {
          write(key, info.get(key));
        }
        indent--;
      }
      List<TTimeSeriesCounter> tsCounters = node.getTime_series_counters();
      if (tsCounters != null && ! tsCounters.isEmpty()) {
        writeLabel("Time Series Counters");
        indent++;
        for (TTimeSeriesCounter counter : tsCounters) {
          writeCounter(counter);
        }
        indent--;
      }
      indent--;
    }

    private void writeCounter(TTimeSeriesCounter counter) {
      List<Long> values = counter.getValues();
      if (values.isEmpty()) {
        return;
      }
      writeIndent();
      out.println(String.format("%s (%s/ %d ms):",
          counter.getName(),
          counter.getUnit().name(),
          counter.getPeriod_ms()));
      indent++;
      int n = Math.min(10, values.size());
      writeIndent();
      for (int i = 0; i < n; i++) {
        if (i > 0) { out.print(" "); }
        out.print(values.get(i));
      }
      if (n < values.size()) {
        out.print(" ...");
      }
      out.println();
      indent--;
    }

    private void writeLabel(String label) {
      writeIndent();
      out.print(label);
      out.println(":");
     }

    private void writeIndent() {
      for (int i = 0; i < indent; i++) {
        out.print("  ");
      }
    }

    private void write(String label, Object value) {
      writeIndent();
      out.print(label);
      out.print(": ");
      out.print(value);
      out.println();
    }
  }

  @Test
  public void testContent() throws IOException {
    File input = new File("/home/progers/data/189495", "impala_profiles_default_1.log");
    LogReader lr = new LogReader(input);
    lr.skip(3);

    QueryRecord qr = lr.next();
    TRuntimeProfileTree profile = qr.thriftProfile();
    new ProfileWalker(profile).convert();
  }

}
