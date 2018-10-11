package com.cloudera.cmf.printer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.shaded.com.google.common.base.Preconditions;

public class TableBuilder {

  public enum Justification {
    LEFT, CENTER, RIGHT
  }

//  public interface TableBuilder {
//    public TableBuilder startHeader();
//    public TableBuilder startRow();
//    public TableBuilder endRow();
//    public TableBuilder startCell();
//    public TableBuilder value(String value);
//    public TableBuilder endCell();
//    public TableBuilder cell(String value);
//    public TableBuilder cell(long value);
//    public TableBuilder cell(Double value);
//    public TableBuilder format(String format);
//  }


  public boolean hasHeader;
  public List<List<String>> data = new ArrayList<>();
  public List<String> row;
  public int col;
  public List<String> formats = new ArrayList<>();
  public List<Justification> justification = new ArrayList<>();

  public TableBuilder startHeader() {
    Preconditions.checkState(! hasHeader);
    hasHeader = true;
    return startRow();
  }

  public TableBuilder startRow() {
    Preconditions.checkState(row == null);
    row = new ArrayList<String>();
    data.add(row);
    col = -1;
    return this;
  }

  public TableBuilder endRow() {
    Preconditions.checkState(! data.isEmpty());

    // If no data, remove the row

    if (row.isEmpty()) {
      // If row was the header, and no data, then
      // just formatting was applied. No actual header.
      if (hasHeader && data.size() ==1) {
        hasHeader= false;
      }
      data.remove(data.size() - 1);
    }
    row = null;
    return this;
  }

  public TableBuilder startCell() {
    Preconditions.checkState(row != null);
    col++;
    return this;
  }

  public TableBuilder value(String value) {
    Preconditions.checkState(row != null);
    for (int i = row.size(); i < col; i++) {
      row.add("");
    }
    if (row.size() <= col) {
      row.add(value);
    } else {
      String cellValue = row.get(row.size() - 1);
      if (cellValue.isEmpty()) {
        cellValue = value;
      } else {
        cellValue += "\n" + value;
      }
      row.set(col, cellValue);
    }
    return this;
  }

  public TableBuilder endCell() {
    return this;
  }

  public TableBuilder cell(String value) {
    Preconditions.checkState(row != null);
    col++;
    value(value);
    return this;
  }

  public TableBuilder cell() {
    return cell("");
  }

  public TableBuilder cell(long value) {
    String format = format();
    if (format == null) {
      cell(Long.toString(value));
    } else {
      cell(String.format(format, value));
    }
    return this;
  }

  public TableBuilder cell(Double value) {
    String format = format();
    if (format == null) {
      cell(Double.toString(value));
    } else {
      cell(String.format(format, value));
    }
    return this;
  }

  private String format() {
    Preconditions.checkState(row != null);
    int posn = row.size();
    if (formats.size() <= posn) { return null; }
    return formats.get(posn);
  }

  public TableBuilder format(String format) {
    Preconditions.checkState(hasHeader);
    Preconditions.checkState(data.size() == 1);
    Preconditions.checkState(row != null);
    for (int i = formats.size(); i < col; i++) {
      formats.add(null);
    }
    if (formats.size() <= col) {
      formats.add(format);
    } else {
      formats.set(col, format);
    }
    return this;
  }

  public TableBuilder justify(Justification just) {
    Preconditions.checkState(hasHeader);
    Preconditions.checkState(data.size() == 1);
    Preconditions.checkState(row != null);
    for (int i = justification.size(); i < col; i++) {
      justification.add(Justification.LEFT);
    }
    if (justification.size() <= col) {
      justification.add(just);
    } else {
      justification.set(col, just);
    }
    return this;
  }

}
