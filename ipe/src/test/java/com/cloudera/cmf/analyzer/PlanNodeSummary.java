package com.cloudera.cmf.analyzer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cmf.printer.AttribBufFormatter;
import com.cloudera.cmf.printer.AttribFormatter;

public class PlanNodeSummary {
  // Operator          #Hosts   Avg Time   Max Time  #Rows  Est. #Rows   Peak Mem  Est. Peak Mem  Detail
  // |--19:EXCHANGE         2   12.135us   12.324us    154         154          0              0  HASH(a16.wshe_number,a16.ws...

  private static String TIME_PATTERN = "(\\S+)\\s+";
  private static String ROW_PATTERN = "([0-9.]+)([A-Z]*)\\s+";
  private static String MEM_PATTERN = "([-0-9.]+) ([A-Z]*)\\s+";
  private static String SUMMARY_PATTERN =
      "[ |-]*(\\d+):[A-Z ]+\\s+" + // Operator
      "(\\d)+\\s+" + // #Hosts
      TIME_PATTERN + // Avg Time
      TIME_PATTERN + // Max Time
      ROW_PATTERN + // #Rows
      ROW_PATTERN + // Est. #Rows
      MEM_PATTERN + // Peak Mem
      MEM_PATTERN + // Est. Peak Mem
      ".*"; // Detail

  public int hostCount;
  public double aveTimeUs;
  public double maxTimeUs;
  public double estRowCount;
  public double actualRowCount;
  public double estMem;
  public double actualMem;

  public int parseSummary(String line) {
    Pattern p = Pattern.compile(SUMMARY_PATTERN);
    Matcher m = p.matcher(line);
    if (! m.matches()) {
      throw new IllegalStateException("Summary line");
    }
    int index = Integer.parseInt(m.group(1));
    hostCount = Integer.parseInt(m.group(2));
    aveTimeUs = ParseUtils.parseDuration(m.group(3));
    maxTimeUs = ParseUtils.parseDuration(m.group(4));
    actualRowCount = ParseUtils.parseRows(m.group(5), m.group(6));
    estRowCount = ParseUtils.parseRows(m.group(7), m.group(8));
    actualMem = ParseUtils.parseMem(m.group(9), m.group(10));
    estMem = ParseUtils.parseMem(m.group(11), m.group(12));
    return index;
  }

  @Override
  public String toString() {
    AttribBufFormatter fmt = new AttribBufFormatter();
    format(fmt);
    return fmt.toString();
  }

  public void format(AttribFormatter fmt) {
    fmt.attrib("Hosts", hostCount);
    fmt.usAttrib("Avg Time (s)", aveTimeUs);
    fmt.usAttrib("Max Time (s)", maxTimeUs);
    fmt.attrib("Rows", actualRowCount);
    fmt.attrib("Rows (est.)", estRowCount);
    fmt.attrib("Peak Memory", actualMem);
    fmt.attrib("Peak Memory (est.)", estMem);
  }
}