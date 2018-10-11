package com.cloudera.cmf.printer;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import com.cloudera.cmf.printer.TableBuilder.Justification;

public class TablePrinter {

  private final TableBuilder table;
  private final PrintWriter out;
  private int colCount;
  private int widths[];
  private String padding;
  private String horizRule;
  private Justification justify[];

  public TablePrinter(TableBuilder table, PrintWriter out) {
    this.table = table;
    this.out = out;
  }

  public TablePrinter(TableBuilder table) {
    this(table, new PrintWriter(
        new OutputStreamWriter(System.out), true));
  }

  public void print() {
    if (table.data.isEmpty()) { return; }
    calcColCount();
    calcWidths();
    calcJustification();
    printHeader();
    int first = firstRow();
    for (int i = first; i < table.data.size(); i++) {
      if (i > first) {
        printHorizBorder("|", "|");
      }
      printRow(table.data.get(i));
    }
    printOuterBorder();
  }

  private void calcColCount() {
    colCount = 0;
    for (int i = 0; i < table.data.size(); i++) {
      colCount = Math.max(colCount, table.data.get(i).size());
    }
  }

  private void calcWidths() {
    widths = new int[colCount];
    for (List<String> row : table.data) {
      for (int i = 0; i < row.size(); i++) {
        calcCellWidth(i, row.get(i));
      }
    }
    int maxWidth = 0;
    for (int i = 0; i < colCount; i++) {
      maxWidth = Math.max(maxWidth, widths[i]);
    }
    padding = repeat(" ", maxWidth);
    horizRule = repeat("-", maxWidth + 2);
  }

  public static String repeat(String str, int n) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < n; i++) {
      buf.append(str);
    }
    return buf.toString();
  }

  private void calcCellWidth(int i, String value) {
    if (value.indexOf('\n') == -1) {
      setCellWidth(i, value);
      return;
    }
    for (String line : value.split("\n")) {
      setCellWidth(i, line);
    }
  }

  private void setCellWidth(int i, String value) {
    widths[i] = Math.max(widths[i], value.length());
  }

  private void calcJustification() {
    justify = new Justification[colCount];
    int n = Math.min(colCount, table.justification.size());
    for (int i = 0; i < n; i++) {
      justify[i] = table.justification.get(i);
    }
    for (int i = n; i < colCount; i++) {
      justify[i] = Justification.LEFT;
    }
  }

  private void printHeader() {
    printOuterBorder();
    if (! table.hasHeader) { return; }
    printRow(table.data.get(0));

    // Heading separator
    for (int i = 0; i < colCount; i++) {
      out.print("|");
      out.print(repeat("=", widths[i] + 2));
    }
    out.println("|");
  }

  private int firstRow() {
    return table.hasHeader ? 1 : 0;
  }

  private void printRow(List<String> row) {
    String cells[][] = new String[colCount][];
    int height = 1;
    for (int i = 0; i < row.size(); i++) {
      cells[i] = row.get(i).split("\n");
      height = Math.max(height, cells[i].length);
    }
    for (int i = row.size(); i < colCount; i++) {
      cells[i] = new String[0];
    }
    for (int line = 0; line < height; line++) {
      for (int col = 0; col < colCount; col++) {
        int index = cells[col].length - height + line;
        if (index < 0) {
          printCell(col, "");
        } else {
          printCell(col, cells[col][index]);
        }
      }
      out.println("|");
    }
  }

  private void printCell(int col, String value) {
    out.print("| ");
    out.print(justifyCell(col, value));
    out.print(" ");
  }

  private String justifyCell(int col, String value) {
    int fill = widths[col] - value.length();
    switch (justify[col]) {
    case CENTER:
      int left = fill / 2;
      return padding(left) + value + padding(fill - left);
    case RIGHT:
      return padding(fill) + value;
    default:
      return value + padding(fill);
    }
  }

  private String padding(int n) {
    return padding.substring(0, n);
  }

  private void printOuterBorder() {
    printHorizBorder("+", "-");
  }

  private void printHorizBorder(String cross, String vert) {
    for (int i = 0; i < colCount; i++) {
      out.print(i == 0 ? cross : vert);
      out.print(horizRule.substring(0, widths[i] + 2));
    }
    out.println(cross);
  }

}
