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

import org.apache.impala.analysis.TableName;
import org.apache.impala.common.ImpalaException;

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
public class DumpParser {

  public interface DumpListener {
    TableName stmt(String stmt) throws ImpalaException;
    void partitionStats(TableName table, List<PartitionDump> partitions) throws ImpalaException;
    void partitionDirs(PartitionDetails details) throws ImpalaException;
    void columnStats(TableName table, List<ColumnStats> cols);
  }

  private enum BlockType {
    EOF, STMT, PART_STATS, PART_DIRS,
    UNPART_DIRS, COL_STATS, IGNORE
  }

  public static class PartitionDump {
    final String key_;
    final List<DumpParser.PartitionDirectoryStats> files_ = new ArrayList<>();

    public PartitionDump(String key) {
      key_ = key;
    }

    public long totalSize() {
      long totalSize = 0;
      for (PartitionDirectoryStats dir : files_) totalSize += dir.sizeBytes;
      return totalSize;
    }
  }

  public static class PartitionDirectoryStats {
    String path_;
    long sizeBytes;

    public PartitionDirectoryStats(String path, String size) {
      path_ = path;
      Pattern p = Pattern.compile("([0-9.]+)([A-Z]*)");
      Matcher m = p.matcher(size);
      if (!m.matches()) {
        throw new IllegalStateException(size);
      }
      sizeBytes = ParseUtils.parseMem(m.group(1), m.group(2));
    }
  }

  private static final String BAR = "\\|";
  private static final String NUMBER_FIELD = " ([-0-9]+)\\s+\\|";
  private static final String FLOAT_FIELD = " ([-0-9.]+)\\s+\\|";
  private static final String TEXT_FIELD = " (\\S+)\\s+\\|";
  private static final String ANY_FIELD = " [^|]+\\|";
  private static final String OPT_FIELD = " ([^|]+)\\s+\\|";

  public static class PartitionDetails {
    private static Pattern PATTERN = Pattern.compile(
        "\\| Total\\s+\\|" +
    NUMBER_FIELD +      // row count
    NUMBER_FIELD +      // fileCount
    TEXT_FIELD +        // Size
    ANY_FIELD + ANY_FIELD + // empty
    ANY_FIELD +        // Format
    ANY_FIELD +        // Incremental Stats
    ANY_FIELD);        // Location

    public final TableName tableName_;
    public final String partitionName_;
    public final long rowCount_;
    public final long fileCount_;
    public final long sizeBytes_;
    public final List<DirectoryDetails> dirs_;

    public PartitionDetails(TableName tableName, String partName,
        String totalLine, List<DirectoryDetails> dirs) {
      tableName_ = tableName;
      partitionName_ = partName;
      dirs_ = dirs;
      if (totalLine == null) {
        rowCount_ = 0;
        fileCount_ = 0;
        sizeBytes_ = 0;
      } else {
        Matcher m = PATTERN.matcher(totalLine);
        if (! m.matches()) {
          throw new IllegalStateException(totalLine);
        }
        int i = 1;
        rowCount_ = Long.parseLong(m.group(i++));
        fileCount_ = Integer.parseInt(m.group(i++));
        sizeBytes_ = ParseUtils.parseMem(m.group(i++));
      }
    }

    public boolean isPartitioned() { return partitionName_ != null; }

    public long rowCount() {
      if (rowCount_ > 0) return rowCount_;
      if (dirs_.isEmpty()) return 0;
      return dirs_.get(0).rowCount_;
    }

    public long sizeBytes() {
      if (sizeBytes_ > 0) return sizeBytes_;
      if (dirs_.isEmpty()) return 0;
      return dirs_.get(0).sizeBytes_;
    }
  }

  public static class DirectoryDetails {
    private static final String FIELDS =
        NUMBER_FIELD +      // row count
        NUMBER_FIELD +      // fileCount
        TEXT_FIELD +        // Size
        ANY_FIELD + ANY_FIELD + // bytes cached, cache replication
        TEXT_FIELD +        // Format
        TEXT_FIELD +        // Incremental Stats
        TEXT_FIELD;        // Location
    private static final Pattern PART_PATTERN = Pattern.compile(
        BAR + TEXT_FIELD +  // Partition name
        FIELDS);
    private static final Pattern UNPART_PATTERN = Pattern.compile(
        BAR + FIELDS);

    public final String key_;
    public final long rowCount_;
    public final int fileCount_;
    public final long sizeBytes_;
    public final String format_;
    public final boolean incrementalStats_;
    public final String location_;

    public DirectoryDetails(boolean partitioned, String line) {
      Pattern p = partitioned ? PART_PATTERN : UNPART_PATTERN;
      Matcher m = p.matcher(line);
      if (! m.matches()) {
        throw new IllegalStateException(line);
      }
      int i = 1;
      key_ = partitioned ? m.group(i++) : null;
      rowCount_ = Long.parseLong(m.group(i++));
      fileCount_ = Integer.parseInt(m.group(i++));
      sizeBytes_ = ParseUtils.parseMem(m.group(i++));
      format_ = m.group(i++);
      incrementalStats_ = Boolean.parseBoolean(m.group(i++));
      location_ = m.group(i++).trim();
    }
  }

  public static class ColumnStats {
    private static final Pattern PATTERN = Pattern.compile(
        BAR + TEXT_FIELD +    // Column
        TEXT_FIELD +          // Type
        NUMBER_FIELD +        // NDV
        NUMBER_FIELD +        // Null count
        NUMBER_FIELD +        // Max Size
        FLOAT_FIELD);        // Avg Size

    public final String name_;
    public final String type_;
    public final long ndv_;
    public final long nullCount_;
    public final int maxWidth;
    public final float avgWidth;

    public ColumnStats(String line) {
      Matcher m = PATTERN.matcher(line);
      if (! m.matches()) {
        throw new IllegalStateException(line);
      }
      int i = 1;
      name_ = m.group(i++);
      type_ = m.group(i++);
      ndv_ = Long.parseLong(m.group(i++));
      nullCount_ = Long.parseLong(m.group(i++));
      maxWidth = Integer.parseInt(m.group(i++));
      avgWidth = Float.parseFloat(m.group(i++));
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
    public TableName stmt(String stmt) {
      stmts.add(stmt);
      return null;
    }

    @Override
    public void partitionStats(TableName table, List<PartitionDump> partitions) {
      throw new IllegalStateException();
    }

    @Override
    public void partitionDirs(PartitionDetails details) {
      throw new IllegalStateException();
    }

    @Override
    public void columnStats(TableName table, List<ColumnStats> cols) {
      throw new IllegalStateException();
    }
  }

  private final DumpListener listener_;
  private BufferedReader in_;
  private TableName currentTable_;
  private String currentPartition_;

  public DumpParser(DumpListener listener) throws FileNotFoundException {
    listener_ = listener;
  }

  /**
   * Shortcut for a smaller file containing only SQL statements.
   * Returns the statements as a list.
   * @throws ImpalaException
   */
  public static List<String> parseDump(File file) throws IOException, ImpalaException {
    StmtListener listener = new StmtListener();
    DumpParser parser = new DumpParser(listener);
    parser.parse(file);
    return listener.stmts;
  }

  public void parse(File file) throws FileNotFoundException, IOException, ImpalaException {
    try (BufferedReader in = new BufferedReader(new FileReader(file))) {
      load(in);
    }
  }

  private void load(BufferedReader in) throws IOException, ImpalaException {
    in_ = in;
    try {
      for (;;) {
        switch(parseHeader()) {
        case EOF:
          return;
        case PART_STATS:
          listener_.partitionStats(currentTable_, parsePartitions());
          break;
        case STMT:
          currentTable_ = listener_.stmt(parseStmt());
          break;
        case PART_DIRS:
          listener_.partitionDirs(parseDirs(true));
          break;
        case UNPART_DIRS:
          listener_.partitionDirs(parseDirs(false));
          break;
        case COL_STATS:
          listener_.columnStats(currentTable_, parseColStats());
          break;
        case IGNORE:
          skipBlock();
          break;
        default:
          throw new IllegalStateException("Unexpected type");
        }
      }
    } finally {
      in_ = null;
    }
  }

  private DumpParser.BlockType parseHeader() throws IOException {
    String line;
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
    }
    if (line == null) return BlockType.EOF;
    line = in_.readLine();
    if (line == null) return BlockType.EOF;
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
      type = BlockType.PART_STATS;
      break;
    case "Column":
      type = BlockType.COL_STATS;
      break;
    case "#Rows":
      type = BlockType.UNPART_DIRS;
      break;
    case "Explain":
      type = BlockType.IGNORE;
      break;
    case "name":
      if (line.contains("| type") && line.contains("| comment")) {
        type = BlockType.IGNORE;
      } else {
        throw new IllegalStateException(m.group(1));
      }
      break;
    default:
      if (line.contains("| #Rows") && line.contains("| Format ")) {
        currentPartition_ = m.group(1);
        type = BlockType.PART_DIRS;
      } else {
        throw new IllegalStateException(m.group(1));
      }
      break;
    }
    line = in_.readLine();
    if (line == null) return BlockType.EOF;
    if (!line.startsWith("+---")) {
      throw new IllegalStateException(m.group(1));
    }
    return type;
  }

  private String parseStmt() throws IOException {
    StringBuilder buf = new StringBuilder();
    String line;
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
      line = line.substring(1, line.length()-1).trim();
      if (line.isEmpty()) continue;
      buf.append(line).append("\n");
    }
    return buf.toString();
  }

  private static final Pattern PARTITION_PATTERN = Pattern.compile(
      BAR + TEXT_FIELD +  // path
      TEXT_FIELD +        // size
      OPT_FIELD);         // key

  private List<PartitionDump> parsePartitions() throws IOException {
    List<PartitionDump> parts = new ArrayList<>();
    DumpParser.PartitionDump part = null;
    String line;
//    Pattern p = Pattern.compile("\\| (\\S+)\\s+\\|\\s+(\\S+)\\s+\\|\\s+(\\S+)\\s+\\|");
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
      Matcher m = PARTITION_PATTERN.matcher(line);
      if (!m.matches()) {
        throw new IllegalStateException(line);
      }
      String key = m.group(3).trim();
      if (part == null || !part.key_.equals(key)) {
        part = new PartitionDump(key);
        parts.add(part);
      }
      part.files_.add(new PartitionDirectoryStats(m.group(1), m.group(2)));
    }
    return parts;
  }

  private void skipRepeatHeader(String expectedHeader) throws IOException {
    String line = in_.readLine();
    if (line == null) {
      throw new IllegalStateException();
    }
    if (! line.startsWith("+---")) {
      throw new IllegalStateException(line);
    }
    line = in_.readLine();
    if (line == null) {
      throw new IllegalStateException();
    }
    if (!line.startsWith("| " + expectedHeader)) {
      throw new IllegalStateException(line);
    }
    line = in_.readLine();
    if (line == null) {
      throw new IllegalStateException();
    }
    if (!line.startsWith("+---")) {
      throw new IllegalStateException(line);
    }
  }

  private PartitionDetails parseDirs(boolean partitioned) throws IOException {
    List<DirectoryDetails> dirs = new ArrayList<>();
    String line;
    while ((line = in_.readLine()) != null) {
//      System.out.println(line);
      if (line.startsWith("| Total")) break;
      if (line.startsWith("+---")) {
        if (!partitioned) break;
        // Repetition of headers
        skipRepeatHeader(currentPartition_);
        continue;
      }
      dirs.add(new DirectoryDetails(partitioned, line));
    }
    PartitionDetails details = new PartitionDetails(currentTable_,
        partitioned ? currentPartition_ : null,
        partitioned ? line : null, dirs);
    if (partitioned) {
    line = in_.readLine();
      if (line != null && !line.startsWith("+---")) {
        throw new IllegalStateException(line);
      }
    }
    return details;
  }

  private List<ColumnStats> parseColStats() throws IOException {
    List<ColumnStats> cols = new ArrayList<>();
    String line;
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
      cols.add(new ColumnStats(line));
    }
    return cols;
  }

  // Read and discard the (name, type, comment) triples, they
  // add no new information.

  private void skipBlock() throws IOException {
    String line;
    while ((line = in_.readLine()) != null) {
      if (line.startsWith("+---")) break;
    }
  }
}