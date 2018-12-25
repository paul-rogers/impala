package org.apache.impala.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a file of possibly-generated SQL statements, applying fix-ups
 * to clean up noise from the generation process.
 */
public class SqlFileParser {
  File file_;
  List<String> stmts_;

  public SqlFileParser(File file) throws IOException {
    file_ = file;
    stmts_ = parseSqlFile(file);
  }

  public static List<String> parse(File file) throws IOException {
    return new SqlFileParser(file).stmts();
  }

  public File file() { return file_; }
  public List<String> stmts() { return stmts_; }

  private List<String> parseSqlFile(File file) throws IOException {
    List<String> stmts = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      for (;;) {
        String stmt = parseSqlStmt(in);
        if (stmt == null) break;
        stmts.add(stmt);
      }
    }
    return stmts;
  }

  private String parseSqlStmt(BufferedReader in) throws IOException {
    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      if (! line.isEmpty()) break;
    }
    if (line == null) return null;
    StringBuilder stmt = new StringBuilder();
    do {
      line = line.trim();
      if (line.isEmpty()) continue;
      if (line.equals("NULL")) continue;
      if (line.endsWith(";")) {
        line = line.substring(0, line.length() - 1);
        stmt.append(line).append("\n");
        break;
      }
      stmt.append(line).append("\n");
    } while ((line = in.readLine()) != null);
    return stmt.toString();
  }
}