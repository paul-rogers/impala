package com.cloudera.cmf;

import static org.junit.Assert.*;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;

import com.cloudera.cmf.printer.TableBuilder;
import com.cloudera.cmf.printer.TableBuilder.Justification;
import com.cloudera.cmf.printer.TablePrinter;

public class TestTablePrinter {

  private void expect(String expected, TableBuilder table) {
    StringWriter out = new StringWriter();
    TablePrinter printer = new TablePrinter(table, new PrintWriter(out));
    printer.print();
    String result = out.toString();
    assertEquals(expected, result);
  }

  @Test
  public void testNoTable() {
    TableBuilder table = new TableBuilder();
    expect("", table);
  }

  @Test
  public void testEmptyTable() {
    TableBuilder table = new TableBuilder()
        .startRow()
        .endRow();
    expect("", table);
  }

  private final String EMPTY_TABLE =
      "+--+\n" +
      "|  |\n" +
      "+--+\n";

  @Test
  public void testEmptyCell() {
    TableBuilder table = new TableBuilder()
        .startRow()
          .cell()
        .endRow();
    expect(EMPTY_TABLE, table);
  }

  private final String ONE_CELL =
      "+-----+\n" +
      "| Foo |\n" +
      "+-----+\n";

  @Test
  public void testOneCellLeft() {
    TableBuilder table = new TableBuilder()
        .startRow()
          .cell("Foo")
        .endRow();
    expect(ONE_CELL, table);
  }

  @Test
  public void testOneCellCenter() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
            .justify(Justification.CENTER)
          .endCell()
        .endRow()
        .startRow()
          .cell("Foo")
        .endRow();
    expect(ONE_CELL, table);
  }

  @Test
  public void testOneCellRight() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
              .justify(Justification.RIGHT)
          .endCell()
        .endRow()
        .startRow()
          .cell("Foo")
        .endRow();
    expect(ONE_CELL, table);
  }

  private final String TWO_BY_TWO =
      "+--------------+\n" +
      "| Fred |       |\n" +
      "|------|-------|\n" +
      "|      | Wilma |\n" +
      "+--------------+\n";

  @Test
  public void testTwoByTwoSimple() {
    TableBuilder table = new TableBuilder()
        .startRow()
          .cell("Fred")
        .endRow()
        .startRow()
          .cell()
          .cell("Wilma")
        .endRow();
    expect(TWO_BY_TWO, table);
  }

  private final String TWO_BY_TWO_LEFT =
      "+--------------+\n" +
      "| Fred | X     |\n" +
      "|------|-------|\n" +
      "| Y    | Wilma |\n" +
      "+--------------+\n";

  @Test
  public void testTwoByTwoLeft() {
    TableBuilder table = new TableBuilder()
        .startRow()
          .cell("Fred")
          .cell("X")
        .endRow()
        .startRow()
          .cell("Y")
          .cell("Wilma")
        .endRow();
    expect(TWO_BY_TWO_LEFT, table);
  }

  private final String TWO_BY_TWO_CENTER =
      "+--------------+\n" +
      "| Fred |   X   |\n" +
      "|------|-------|\n" +
      "|  Y   | Wilma |\n" +
      "+--------------+\n";

  @Test
  public void testTwoByTwoCenter() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
            .justify(Justification.CENTER)
          .endCell()
          .startCell()
            .justify(Justification.CENTER)
          .endCell()
        .endRow()
        .startRow()
          .cell("Fred")
          .cell("X")
        .endRow()
        .startRow()
          .cell("Y")
          .cell("Wilma")
        .endRow();
    expect(TWO_BY_TWO_CENTER, table);
  }

  private final String TWO_BY_TWO_RIGHT =
      "+--------------+\n" +
      "| Fred |     X |\n" +
      "|------|-------|\n" +
      "|    Y | Wilma |\n" +
      "+--------------+\n";

  @Test
  public void testTwoByTwoRight() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
            .justify(Justification.RIGHT)
          .endCell()
          .startCell()
            .justify(Justification.RIGHT)
          .endCell()
        .endRow()
        .startRow()
          .cell("Fred")
          .cell("X")
        .endRow()
        .startRow()
          .cell("Y")
          .cell("Wilma")
        .endRow();
    expect(TWO_BY_TWO_RIGHT, table);
  }

  private final String TWO_WITH_HEADER =
      "+-----------------+\n" +
      "| Husband | Wife  |\n" +
      "|=========|=======|\n" +
      "| Fred    |       |\n" +
      "|---------|-------|\n" +
      "|         | Wilma |\n" +
      "+-----------------+\n";

  @Test
  public void testTwoWithHeader() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .cell("Husband")
          .cell("Wife")
        .endRow()
        .startRow()
          .cell("Fred")
        .endRow()
        .startRow()
          .cell()
          .cell("Wilma")
        .endRow();
    expect(TWO_WITH_HEADER, table);
  }

  private final String MULTI_LINE_ALL =
      "+-----------------------------+\n" +
      "|            |        | Third |\n" +
      "| First      | Second |   X   |\n" +
      "|============|========|=======|\n" +
      "| Fred       |        |       |\n" +
      "| Flintstone |      1 | Good  |\n" +
      "|------------|--------|-------|\n" +
      "| Barney     |        |  Not  |\n" +
      "| Rubble     |     20 |  Bad  |\n" +
      "+-----------------------------+\n";

  @Test
  public void testMultiLine() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
            .value("First")
          .endCell()
          .startCell()
            .value("Second")
            .justify(Justification.RIGHT)
          .endCell()
          .startCell()
            .value("Third")
            .value("X")
            .justify(Justification.CENTER)
          .endCell()
        .endRow()
        .startRow()
          .startCell() // Multi-value
            .value("Fred")
            .value("Flintstone")
          .endCell()
          .cell(1) // No format
          .cell("Good")
        .endRow()
        .startRow()
          .cell("Barney\nRubble") // Multi-line
          .cell(20) // No format
          .cell("Not\nBad")
        .endRow();
    expect(MULTI_LINE_ALL, table);
  }

  private final String FORMAT =
      "+---------------------+\n" +
      "|    123 |       1.01 |\n" +
      "|--------|------------|\n" +
      "| 12,345 | 123,456.78 |\n" +
      "+---------------------+\n";

  @Test
  public void testFormat() {
    TableBuilder table = new TableBuilder()
        .startHeader()
          .startCell()
            .justify(Justification.RIGHT)
            .format("%,d")
          .startCell()
          .endCell()
            .justify(Justification.RIGHT)
            .format("%,.2f")
          .endCell()
        .endRow()
        .startRow()
          .cell(123)
          .cell(1.01)
        .endRow()
        .startRow()
          .cell(12_345)
          .cell(123_456.78)
        .endRow();
    expect(FORMAT, table);
  }

}
