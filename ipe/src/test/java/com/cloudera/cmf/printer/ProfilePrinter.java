package com.cloudera.cmf.printer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TEventSequence;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.impala.thrift.TSummaryStatsCounter;
import org.apache.impala.thrift.TTimeSeriesCounter;

import com.cloudera.cmf.profile.ParseUtils;

/**
   * Basic dump of a query profile to an output stream. Format is much
   * like that in the Impala UI, but without aggregations. Used to explore
   * the profile structure during development.
   */
  public class ProfilePrinter {

    private final TRuntimeProfileTree profile;
    private AttribFormatter fmt;

    public ProfilePrinter(TRuntimeProfileTree profile, AttribFormatter fmt) {
      this.profile = profile;
      this.fmt = fmt;
    }

    public ProfilePrinter(TRuntimeProfileTree profile) {
      this(profile,
          new AttribPrintFormatter());
    }

    public void convert() {
      fmt.startGroup("Query");
      convertNode(profile.getNodesIterator());
      fmt.endGroup();
    }

    private void convertNode(Iterator<TRuntimeProfileNode> iter) {
      assert iter.hasNext();
      TRuntimeProfileNode node = iter.next();
      int childCount = node.getNum_children();
      convertNode(node);
      if (node.isIndent()) { fmt.startGroup(); }
      for (int i = 0; i < childCount; i++) {
        convertNode(iter);
      }
      if (node.isIndent()) { fmt.endGroup(); }
    }

    public void convertNode(TRuntimeProfileNode node) {
      fmt.startGroup(node.getName());
//      fmt.attrib("Node ID", node.getMetadata());
//      fmt.attrib("Child Count", node.getNum_children());
      writeAttribs(node);
      writeCounters(node);
      writeTimeSeries(node);
      writeSummaryStats(node);
      writeEvents(node);
      fmt.endGroup();
    }

    private void writeAttribs(TRuntimeProfileNode node) {
      Map<String, String> info = node.getInfo_strings();
      List<String> keys = node.getInfo_strings_display_order();
      if (info.size() != keys.size()) {
        fmt.line(String.format("WARNING: map size = %d, key size = %d",
            info.size(), keys.size()));
      }
      if (! info.isEmpty()) {
        for (String key : keys) {
          fmt.attrib(key, info.get(key));
        }
      }
    }

    private void writeCounters(TRuntimeProfileNode node) {
      List<TCounter> counters = node.getCounters();
      if (counters != null && ! counters.isEmpty()) {
        fmt.startGroup("Counters");
        for (TCounter counter : counters) {
          writeCounter(counter);
        }
        fmt.endGroup();
      }
    }

    private void writeCounter(TCounter counter) {
      fmt.attrib(counter.getName(),
          String.format("%d %s",
              counter.getValue(),
              counter.getUnit().name()));
    }

    private void writeTimeSeries(TRuntimeProfileNode node) {
      List<TTimeSeriesCounter> tsCounters = node.getTime_series_counters();
      if (tsCounters == null || tsCounters.isEmpty()) {
        return;
      }
      fmt.startGroup("Time Series Counters");
      for (TTimeSeriesCounter counter : tsCounters) {
        writeTimeSeriesCounter(counter);
      }
      fmt.endGroup();;
    }

    private void writeTimeSeriesCounter(TTimeSeriesCounter counter) {
      List<Long> values = counter.getValues();
      if (values.isEmpty()) {
        return;
      }
      fmt.startGroup(String.format("%s (%s / %d ms):",
          counter.getName(),
          counter.getUnit().name(),
          counter.getPeriod_ms()));
      int n = Math.min(10, values.size());
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < n; i++) {
        if (i > 0) { buf.append(" "); }
        buf.append(values.get(i));
      }
      if (n < values.size()) {
        buf.append(" ...");
      }
      fmt.line(buf.toString());
      fmt.endGroup();
    }

    private void writeSummaryStats(TRuntimeProfileNode node) {
      List<TSummaryStatsCounter> sCounters = node.getSummary_stats_counters();
      if (sCounters == null || sCounters.isEmpty()) {
        return;
      }
      fmt.startGroup("Summary Counters");
      for (TSummaryStatsCounter counter : sCounters) {
        writeSummaryCounter(counter);
      }
      fmt.endGroup();
    }

    private void writeSummaryCounter(TSummaryStatsCounter counter) {
      fmt.startGroup(counter.name);
      fmt.attrib("Unit", counter.getUnit().name());
      fmt.attrib("Sum", counter.getSum());
      fmt.attrib("Count", counter.getTotal_num_values());
      fmt.attrib("Min", counter.getMin_value());
      fmt.attrib("Max", counter.getMax_value());
      fmt.endGroup();
    }

    private void writeEvents(TRuntimeProfileNode node) {
      List<TEventSequence> eventSeqs = node.getEvent_sequences();
      if (eventSeqs != null && ! eventSeqs.isEmpty()) {
        fmt.startGroup("Event Sequences");
        for (TEventSequence eventSeq : eventSeqs) {
          writeEventSequence(eventSeq);
        }
        fmt.endGroup();
      }
    }

    private void writeEventSequence(TEventSequence eventSeq) {
      List<Long> timestamps = eventSeq.getTimestamps();
      List<String> labels = eventSeq.getLabels();
      assert(timestamps.size() == labels.size());
      if (timestamps.isEmpty()) {
        return;
      }
      long startTime = timestamps.get(0);
      long totalNs = timestamps.get(timestamps.size()-1) - startTime;

      fmt.startGroup(
          String.format("%s: %s",
              eventSeq.getName(),
              ParseUtils.formatNS(totalNs)));
      for (int i = 0; i < timestamps.size(); i++) {
        fmt.attrib(labels.get(i),
            ParseUtils.formatNS(timestamps.get(i) - startTime));
      }
      fmt.endGroup();
    }
  }