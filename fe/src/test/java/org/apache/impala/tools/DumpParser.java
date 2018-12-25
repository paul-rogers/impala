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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses an Impala Shall metadata dump which can contain:
 *
 * * SQL statements enclosed in a "results" box.
 * * Partition dumps
 * * File dumps
 *
 * When running a statement in the shell, the output does not
 * tell us what table appeared in the select. Would be better to
 * echo the query itself in the output. We assume that the
 * CREATE TABLE statement appears first, then all metadata dumps
 * afterwards are for that same table.
 *
 * Since dump files tend to be huge, the parser materializes only
 * one item at a time, then calls a listener method for that item.
 */
public class DumpParser implements AutoCloseable {

  public interface DumpListener {
    String stmt(String stmt);
    void tableDetails(String table, List<DumpParser.PartitionDump> partitions);
  }

  private enum BlockType {
    EOF, STMT, PARITION_DUMP
  }

  public static class PartitionDump {
    final String name_;
    final List<DumpParser.FileDump> files_ = new ArrayList<>();

    public PartitionDump(String name) {
      name_ = name;
    }
  }

  public static class FileDump {
    String path_;
    long sizeBytes;

    public FileDump(String path, String size) {
      path_ = path;
      Pattern p = Pattern.compile("([0-9.]+)([A-Z]*)");
      Matcher m = p.matcher(size);
      if (!m.matches()) {
        throw new IllegalStateException(size);
      }
      sizeBytes = ParseUtils.parseMem(m.group(1), m.group(2));
    }
  }

  /**
   * Specialized parser for a file that contains only SQL statements.
   * Gathers them into a list. Fails if the file turns out to contain
   * other kinds of information.
   */
  public static class StmtListener implements DumpListener {
    List<String> stmts = new ArrayList<>();

    @Override
    public String stmt(String stmt) {
      stmts.add(stmt);
      return null;
    }

    @Override
    public void tableDetails(String table, List<PartitionDump> partitions) {
      throw new IllegalStateException();
    }
  }

  private final File dumpFile_;
  private final DumpListener listener_;
  private final BufferedReader in_;

  public DumpParser(File dumpFile, DumpListener listener) throws FileNotFoundException {
    dumpFile_ = dumpFile;
    listener_ = listener;
    in_ = new BufferedReader(new FileReader(dumpFile));
  }

  /**
   * Shortcut for a smaller file containing only SQL statements.
   * Returns the statements as a list.
   */
  public static List<String> parseDump(File file) throws IOException {
    StmtListener listener = new StmtListener();
    try (DumpParser parser = new DumpParser(file, listener)) {
      parser.load();
    }
    return listener.stmts;
  }

  public void load() throws IOException {
    String currentTable = null;
    for (;;) {
      switch(parseHeader()) {
      case EOF:
        return;
      case PARITION_DUMP:
        listener_.tableDetails(currentTable, parseFiles());
        break;
      case STMT:
        currentTable = listener_.stmt(parseStmt());
        break;
      default:
        throw new IllegalStateException("Unexpected type");
      }
    }
  }

  private DumpParser.BlockType parseHeader() throws IOException {
    String line;
    while ((line = in_.readLine()) != null) {
      if (!line.startsWith("+---")) continue;
      line = in_.readLine();
      if (line == null) break;
      Pattern p = Pattern.compile("^\\| (\\S+) ");
      Matcher m = p.matcher(line);
      if (! m.find()) {
        throw new IllegalStateException(line);
      }
      DumpParser.BlockType type;
      switch(m.group(1)) {
      case "result":
        type = BlockType.STMT;
        break;
      case "Path":
        type = BlockType.PARITION_DUMP;
        break;
      default:
        throw new IllegalStateException(m.group(1));
      }
      line = in_.readLine();
      if (line == null) break;
      if (!line.startsWith("+---")) continue;
      return type;
    }
    return BlockType.EOF;
  }

  private String parseStmt() throws IOException {
    StringBuilder buf = new StringBuilder();
    String line;
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
      line = line.substring(1, line.length()-2).trim();
      if (line.isEmpty()) continue;
      buf.append(line).append("\n");
    }
    return buf.toString();
  }

  private List<DumpParser.PartitionDump> parseFiles() throws IOException {
    List<DumpParser.PartitionDump> parts = new ArrayList<>();
    DumpParser.PartitionDump part = null;
    String line;
    Pattern p = Pattern.compile("\\| (\\S+)\\s+\\|\\s+(\\S+)\\s+\\|\\s+(\\S+)\\s+\\|");
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
      Matcher m = p.matcher(line);
      if (!m.matches()) {
        throw new IllegalStateException(line);
      }
      String partName = m.group(3);
      if (part == null || !part.name_.equals(partName)) {
        part = new PartitionDump(partName);
        parts.add(part);
      }
      part.files_.add(new FileDump(m.group(1), m.group(2)));
    }
    return parts;
  }


  @Override
  public void close() throws IOException {
    in_.close();
  }
}