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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

public class PlanAnalysisUtils {

  /**
   * Holds the parse of one line of the summary table.
   * Fields are available in both the original text form
   * and in a parsed, numeric form.
   */
  public static class SummaryNode {
    // Operator          #Hosts   Avg Time   Max Time  #Rows  Est. #Rows   Peak Mem  Est. Peak Mem  Detail
    // |--19:EXCHANGE         2   12.135us   12.324us    154         154          0              0  HASH(a16.wshe_number,a16.ws...

    private static final String FIELD = "(\\S+)\\s+";
    private static final String MEM_PATTERN = "([-0-9.]+ [A-Z]*)\\s+";
    private static String SUMMARY_PATTERN =
        "([ |-]*)(\\d+):([A-Z ]+)\\s+" + // Operator
        FIELD + // #Hosts
        FIELD + // Avg Time
        FIELD + // Max Time
        FIELD + // #Rows
        FIELD + // Est. #Rows
        MEM_PATTERN + // Peak Mem
        MEM_PATTERN + // Est. Peak Mem
        "(.*)"; // Detail

    public String prefix;
    public String idStr;
    public String label;
    public int hostCount;
    public String avgTime;
    public String maxTime;
    public String actualCardinality;
    public String estCardinality;
    public String actualPeakMem;
    public String estPeakMem;
    public String detail;

    private SummaryNode(Matcher m) {
      int i = 1;
      prefix = m.group(i++);
      idStr = m.group(i++);
      label = m.group(i++).trim();
      hostCount = Integer.parseInt(m.group(i++));
      avgTime = m.group(i++);
      maxTime = m.group(i++);
      actualCardinality = m.group(i++);
      estCardinality = m.group(i++);
      actualPeakMem = m.group(i++).trim();
      estPeakMem = m.group(i++).trim();
      detail = m.group(i++).trim();
    }

    public static SummaryNode parse(String line) {
      Pattern p = Pattern.compile(SUMMARY_PATTERN);
      Matcher m = p.matcher(line);
      if (! m.matches()) {
        return null;
      }
      return new SummaryNode(m);
    }

    public int depth() {
      if (prefix.isEmpty()) return 0;
      if (prefix.length() == 2) return 1;
      return (prefix.length() - 2) / 3 + 1;
    }

    public int id() {
      return Integer.parseInt(idStr);
    }

    public double avgTimeUs() {
      return ParseUtils.parseDuration(avgTime);
    }

    public double maxTimeUs() {
      return ParseUtils.parseDuration(maxTime);
    }

    public long actualRowCount() {
      return ParseUtils.parseRows(actualCardinality);
    }

    public long estRowCount() {
      return ParseUtils.parseRows(estCardinality);
    }

    public long estMemBytes() {
      return ParseUtils.parseMem(estPeakMem);
    }
  }

  public static List<SummaryNode> parseSummary(BufferedReader in) throws IOException {
    String line = in.readLine();
    if (line == null) return null;
    if (!line.startsWith("Operator")) {
      throw new IllegalStateException(line);
    }
    line = in.readLine();
    if (line == null) return null;
    if (!line.startsWith("-----")) {
      throw new IllegalStateException(line);
    }
    List<SummaryNode> nodes = new ArrayList<>();
    while ((line = in.readLine()) != null) {
      SummaryNode node = SummaryNode.parse(line);
      if (node == null) {
        throw new IllegalStateException(line);
      }
      nodes.add(node);
    }
    return nodes;
  }

  public static List<SummaryNode> parseSummary(String table) {
    try (BufferedReader in = new BufferedReader(new StringReader(table))) {
      return parseSummary(in);
    } catch (IOException e) {
      // Should never occur
      throw new IllegalStateException(e);
    }
  }

  public static class CardinalityNode {
    int depth_;
    String id_;
    String label_;
    String actualCard_;
    String estCard_;
    String table_;

    public CardinalityNode(SummaryNode node) {
      depth_ = node.depth();
      id_ = node.idStr;
      label_ = node.label;
      actualCard_ = node.actualCardinality;
      estCard_ = node.estCardinality;
    }
  }

  /**
   * A cardinality model for a plan node. Abstracts out just the drivers
   * of cardinality: the operator, input table, estimated and actual
   * cardinalities. Used to create a summary table showing just this
   * information.
   */
  public static class CardinalityModel {

    Map<String, CardinalityNode> nodes = new LinkedHashMap<>();

    public CardinalityModel(ProfileParser pp) {
      parseTable(pp.summary());
      parsePlan(pp.plan());
    }

    private void parseTable(String summary) {
      for (SummaryNode tableNode : parseSummary(summary)) {
        nodes.put(tableNode.idStr, new CardinalityNode(tableNode));
      }
    }

    private void parsePlan(String plan) {
      Pattern p = Pattern.compile("[- |]*(F?\\d+):SCAN [A-Z ]*\\[([^,\\]]*)");
      try (BufferedReader in = new BufferedReader(new StringReader(plan))) {
        String line;
        while ((line = in.readLine()) != null) {
          Matcher m = p.matcher(line);
          if (!m.find()) continue;
          CardinalityNode node = nodes.get(m.group(1));
          Preconditions.checkNotNull(node);
          node.table_ = m.group(2);
        }
      } catch (IOException e) {
        // Should never occur
        throw new IllegalStateException(e);
      }
    }

    public static String LAYOUT = "%-26s  %8s  %8s  %s";

    public void writeTable(PrintWriter out) {
      out.println(String.format(LAYOUT,
          "Operation", "Card", "Est Card", "Table"));
      out.println(QueryUtils.repeat("-", 26 + 1 + 8 + 2 + 8 + 2 + 20));
      for (CardinalityNode node : nodes.values()) {
        out.println(String.format(LAYOUT,
            QueryUtils.repeat("  ", node.depth_) + node.label_,
            node.actualCard_, node.estCard_,
            node.table_ == null ? "" : node.table_));
      }
    }

    @Override
    public String toString() {
      StringWriter strWriter = new StringWriter();
      PrintWriter out = new PrintWriter(strWriter);
      writeTable(out);
      out.close();
      return strWriter.toString();
    }
  }

  public static final String PLAN = "Plan:";
  public static final String ROOT = "PLAN-ROOT SINK";
  public static final String WARNING = "WARNING: ";

  /**
   * Given a full EXPLAIN plan, reduce it to remove fragments and
   * exchanges, leaving just the essence of the plan for easier analysis.
   *
   * @param fullPlan the full EXPLAIN plan
   * @return the reduced plan with only essential nodes and data
   */
  public static String reduce(String fullPlan) {
    StringBuilder buf = new StringBuilder();
    Pattern p = Pattern.compile("([-| ]*)((F?\\d+:)?(.*))?");

    // Skip to PLAN_ROOT SINK
    try (BufferedReader in = new BufferedReader(new StringReader(fullPlan))) {
      String line;
      while ((line = in.readLine()) != null) {
        if (line.equals(ROOT)) {
          buf.append(line).append("\n");
          break;
        }
        if (line.startsWith(WARNING)) { break; }
        // 5.4 has no root
        if (line.isEmpty()) {
          buf.append(line).append("\n");
          break;
        }
      }
      if (line == null) {
        throw new IllegalStateException("Malformed profile");
      }
      if (line.startsWith(WARNING)) {
        buf.append(line).append("\n");
        while ((line = in.readLine()) != null) {
          buf.append(line).append("\n");
          if (line.equals(ROOT) || line.isEmpty()) { break; }
        }
      }
      // Parse plan lines
      String prefix = null;
      boolean skip = false;
      boolean ignoreOp = false;
      while ((line = in.readLine()) != null) {
        Matcher m = p.matcher(line);
        if (! m.matches()) continue;
        String lead = m.group(1);
        String tail = m.group(2);
        String id = m.group(3);
        String op = m.group(4);
        if (tail == null ||tail.isEmpty()) {
          if (! skip && !ignoreOp) {
            buf.append(line).append("\n");
            skip = true;
          }
          continue;
        }
        if (id == null || id.isEmpty()) {
          if (ignoreOp) continue;
          if (op.startsWith("Per-Host")) continue;
          if (op.contains("stats:")) continue;
          if (op.startsWith("stats")) continue;
          if (op.startsWith("in pipelines:")) continue;
          if (op.startsWith("mem-estimate=")) continue;
          if (op.startsWith("runtime filters:")) continue;
          if (op.startsWith("predicates")) continue;
          if (op.startsWith("partition predicates:")) continue;
          if (op.startsWith("hash predicates:")) continue;
          if (op.startsWith("fk/pk conjuncts:")) continue;
          if (op.startsWith("output:")) continue;
          if (op.startsWith("group by")) continue;
          if (op.startsWith("partitions:")) continue;
          if (op.startsWith("columns:")) continue;
          if (op.startsWith("extrapolated-rows=")) continue;
          if (op.startsWith("parquet statistics predicates:")) continue;
          if (op.startsWith("parquet dictionary predicates:")) continue;
          buf.append(lead).append(op).append("\n");
          continue;
        }
        if (id.startsWith("F")) continue;
        if (prefix == null) prefix = lead;
        if (lead.length() != prefix.length()) prefix = lead;
        if (!lead.endsWith(" ")) prefix = lead;
        ignoreOp = op.startsWith("EXCHANGE");
        if (ignoreOp) continue;
        buf.append(prefix).append(tail).append("\n");
        skip = false;
      }
    } catch (IOException e) {
      // Should never occur
    }
    return buf.toString();
  }

  public static class CardinalityError {
    private static class RowSummary {
      SummaryNode node_;
      double actual;
      double est;
      double error;
      int logError;

      public RowSummary(SummaryNode node) {
        node_ = node;
        actual = node.actualRowCount();
        est = node.estRowCount();
        if (est < 0) {
          error = 0;
          logError = 0;
        } else {
          double baseActual = actual <= 0 ? 1 : actual;
          double baseEst = est <= 0 ? 1 : est;
          error = baseEst / baseActual;
          logError = (int) Math.round(Math.log10(error));
        }
      }
    }

    List<RowSummary> rows = new ArrayList<>();
    int minLogError;
    int maxLogError;

    public CardinalityError(String summary) {
      List<SummaryNode> nodes = parseSummary(summary);
      for (SummaryNode node : nodes) {
        RowSummary row = new RowSummary(node);
        rows.add(row);
        minLogError = Math.min(minLogError, row.logError);
        maxLogError = Math.max(maxLogError, row.logError);
      }
    }

    private static final String CARD_FORMAT = "%-26s  %8s  %8s  ";

    public String printTable() {
      StringBuilder buf = new StringBuilder();
      buf.append(String.format(CARD_FORMAT + "    %s",
          "Operation", "Card", "Est Card", "log10(error)"))
         .append("\n")
         .append(QueryUtils.repeat("-", 64 +
             Math.max(0, maxLogError - minLogError - 6)))
         .append("\n");
      for (RowSummary row : rows) {
        buf.append(String.format(CARD_FORMAT + "  %3d  ",
            row.node_.prefix + row.node_.idStr + ":" + row.node_.label,
            row.node_.actualCardinality, row.node_.estCardinality,
            row.logError));
        if (row.est < 0) {
          buf.append(QueryUtils.repeat(" ", -minLogError - 1)).append("???");
        } else {
          buf.append(QueryUtils.horizBarChart(minLogError, maxLogError, row.logError));
        }
        buf.append("\n");
      }
      return buf.toString();
    }
  }

  public static String cardinalityErrorTable(String summaryTable) {
    return new CardinalityError(summaryTable).printTable();
  }
}
