package com.cloudera.cmf.printer;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class AttribPrintFormatter extends AbstractAttribFormatter {

  private final PrintWriter out;

  public AttribPrintFormatter(PrintWriter out) {
    this.out = out;
  }

  public AttribPrintFormatter() {
    this(new PrintWriter(
        new OutputStreamWriter(System.out), true));
  }

  @Override
  public void startGroup(String text) {
    startLine();
    out.println(text);
    startGroup();
  }

  @Override
  public void attrib(String label, String value) {
    startLine();
    out.print(label);
    out.print(": ");
    out.println(value == null ? "null" : value.toString());
  }

  private void startLine() {
    for (int i = 0; i < indent; i++) {
      out.print("  ");
    }
  }

  @Override
  public void line(String line) {
    startLine();
    out.println(line);
  }
}
