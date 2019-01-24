package org.apache.impala.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProfileParser {
  private final File file_;
  private final String version_;
  private final String query_;
  private final String plan_;
  private final String execSummary_;

  public ProfileParser(File file) throws FileNotFoundException, IOException {
    file_ = file;
    try (BufferedReader in = new BufferedReader(new FileReader(file_))) {
      version_ = parseVersion(in);
      query_ = parseQuery(in);
      plan_ = parsePlan(in);
      execSummary_ = parseTable(in);
    }
  }

  public String version() { return version_; }
  public String query() { return query_; }
  public String plan() { return plan_; }
  public String summary() { return execSummary_; }

  private static final String VERSION_PREFIX = "    Impala Version: ";
  private static final String QUERY_PREFIX = "    Sql Statement: ";
  private static final String END_QUERY_MARKER = "    Coordinator: ";

  private String parseVersion(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      if (line.startsWith(VERSION_PREFIX)) break;
    }
    if (line == null) {
      throw new IllegalArgumentException("Query not found in profile");
    }
    Pattern p = Pattern.compile("impalad version (\\S+) RELEASE");
    Matcher m = p.matcher(line);
    if (! m.find()) {
      throw new IllegalArgumentException("Version not found in line: " + line);
    }
    return m.group(1);
  }

  private String parseQuery(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      if (line.startsWith(QUERY_PREFIX)) break;
    }
    if (line == null) {
      throw new IllegalArgumentException("Query not found in profile");
    }
    StringBuilder buf = new StringBuilder();
    buf.append(line.substring(QUERY_PREFIX.length())).append("\n");
    while ((line = in.readLine()) != null) {
      if (line.startsWith(END_QUERY_MARKER)) break;
      buf.append(line).append("\n");
    }
    if (line == null) {
      throw new IllegalArgumentException("EOF while reading query");
    }
    // Input positioned just after Coordinator: line
    return buf.toString();
  }

  private static final String PLAN_MARKER = "    Plan:";
  private static final String END_PLAN_MARKER = "----------------";

  private String parsePlan(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      if (line.startsWith(PLAN_MARKER)) break;
    }
    if (line == null) {
      throw new IllegalArgumentException("Plan not found in profile");
    }
    // Skip ruling and resource lines
    while ((line = in.readLine()) != null) {
      if (line.isEmpty()) break;
    }
    StringBuilder buf = new StringBuilder();
    buf.append(line).append("\n");
    while ((line = in.readLine()) != null) {
      if (line.startsWith(END_PLAN_MARKER)) break;
      buf.append(line).append("\n");
    }
    if (line == null) {
      throw new IllegalArgumentException("EOF while reading plan");
    }
    // Input positioned just after the end ruling
    return buf.toString();
  }

  private static final String TABLE_MARKER = "    ExecSummary:";

  private String parseTable(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      if (line.startsWith(TABLE_MARKER)) break;
    }
    if (line == null) {
      throw new IllegalArgumentException("Exec summaray not found in profile");
    }
    StringBuilder buf = new StringBuilder();
    while ((line = in.readLine()) != null) {
      if (line.startsWith("  ")) break;
      buf.append(line).append("\n");
    }
    if (line == null) {
      throw new IllegalArgumentException("EOF while reading exec summary");
    }
    // Input positioned just after the end ruling
    return buf.toString();
  }
}