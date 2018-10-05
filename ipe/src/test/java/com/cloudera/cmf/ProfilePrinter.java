package com.cloudera.cmf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TSummaryStatsCounter;
import org.apache.impala.thrift.TTimeSeriesCounter;

/**
   * Basic dump of a query profile to an output stream. Format is much
   * like that in the Impala UI, but without aggregations. Used to explore
   * the profile structure during development.
   */
  public class ProfilePrinter {

    private final TRuntimeProfileTree profile;
    private final PrintWriter out;
    int indent;

    public ProfilePrinter(TRuntimeProfileTree profile, PrintWriter out) {
      this.profile = profile;
      this.out = out;
    }

    public ProfilePrinter(TRuntimeProfileTree profile) {
      this(profile,
          new PrintWriter(new OutputStreamWriter(System.out), true));
    }

    public ProfilePrinter(TRuntimeProfileTree profile, File file) throws IOException {
      this(profile,
          new PrintWriter(new FileWriter(file)));
    }

    public void convert() {
      writeLabel("Query");
      indent++;
      convertNode(profile.getNodesIterator());
      indent--;
      out.flush();
    }

    private void convertNode(Iterator<TRuntimeProfileNode> iter) {
      assert iter.hasNext();
      TRuntimeProfileNode node = iter.next();
      int childCount = node.getNum_children();
      convertNode(node);
      if (node.isIndent()) { indent++; }
      for (int i = 0; i < childCount; i++) {
        convertNode(iter);
      }
      if (node.isIndent()) { indent--; }
    }

    public void convertNode(TRuntimeProfileNode node) {
      writeLabel(node.getName());
      indent++;
      write("Node ID", node.getMetadata());
      write("Child Count", node.getNum_children());
      writeAttribs(node);
      writeCounters(node);
      writeTimeSeries(node);
      writeSummaryStats(node);
      writeEvents(node);
    }

    private void writeAttribs(TRuntimeProfileNode node) {
      Map<String, String> info = node.getInfo_strings();
      List<String> keys = node.getInfo_strings_display_order();
      if (info.size() != keys.size()) {
        writeIndent();
        out.print(String.format("WARNING: map size = %d, key size = %d",
            info.size(), keys.size()));
      }
      if (! info.isEmpty()) {
//        writeLabel("Info");
//        indent++;
        for (String key : keys) {
          write(key, info.get(key));
        }
//        indent--;
      }
    }

    private void writeCounters(TRuntimeProfileNode node) {
      List<TCounter> counters = node.getCounters();
      if (counters != null && ! counters.isEmpty()) {
        writeLabel("Counters");
        indent++;
        for (TCounter counter : counters) {
          writeCounter(counter);
        }
        indent--;
      }
    }

    private void writeCounter(TCounter counter) {
      writeIndent();
      out.println(String.format("%s: %d %s",
          counter.getName(), counter.getValue(),
          counter.getUnit().name()));
    }

    private void writeTimeSeries(TRuntimeProfileNode node) {
      List<TTimeSeriesCounter> tsCounters = node.getTime_series_counters();
      if (tsCounters == null || tsCounters.isEmpty()) {
        return;
      }
      writeLabel("Time Series Counters");
      indent++;
      for (TTimeSeriesCounter counter : tsCounters) {
        writeTimeSeriesCounter(counter);
      }
      indent--;
    }

    private void writeTimeSeriesCounter(TTimeSeriesCounter counter) {
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

    private void writeSummaryStats(TRuntimeProfileNode node) {
      List<TSummaryStatsCounter> sCounters = node.getSummary_stats_counters();
      if (sCounters == null || sCounters.isEmpty()) {
        return;
      }
      writeLabel("Summary Counters");
      indent++;
      for (TSummaryStatsCounter counter : sCounters) {
        writeSummaryCounter(counter);
      }
      indent--;
    }

    private void writeSummaryCounter(TSummaryStatsCounter counter) {
      writeLabel(counter.name);
      indent++;
      write("Unit", counter.getUnit().name());
      write("Sum", counter.getSum());
      write("Count", counter.getTotal_num_values());
      write("Min", counter.getMin_value());
      write("Max", counter.getMax_value());
      indent--;
    }

    private void writeEvents(TRuntimeProfileNode node) {
      List<TEventSequence> eventSeqs = node.getEvent_sequences();
      if (eventSeqs != null && ! eventSeqs.isEmpty()) {
        writeLabel("Event Sequences");
        indent++;
        for (TEventSequence eventSeq : eventSeqs) {
          writeEventSequence(eventSeq);
        }
        indent--;
      }
      indent--;
    }

    private void writeEventSequence(TEventSequence eventSeq) {
      List<Long> timestamps = eventSeq.getTimestamps();
      List<String> labels = eventSeq.getLabels();
      assert(timestamps.size() == labels.size());
      if (timestamps.isEmpty()) {
        return;
      }
      long startTime = timestamps.get(0);
      long totalMs = timestamps.get(timestamps.size()-1) - startTime;

      writeMs(eventSeq.getName(), totalMs);
      indent++;
      for (int i = 0; i < timestamps.size(); i++) {
        writeMs(labels.get(i),
            timestamps.get(i) - startTime);
      }
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

    public void writeMs(String label, long ms) {
      write(label,
          String.format("%,.3f s", ms / 1000.0));
    }

    public void close() {
      out.close();
    }
  }