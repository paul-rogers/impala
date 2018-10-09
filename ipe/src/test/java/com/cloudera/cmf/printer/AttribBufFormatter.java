package com.cloudera.cmf.printer;

import java.io.PrintWriter;

public class AttribBufFormatter extends AbstractAttribFormatter {

  private final StringBuilder buf = new StringBuilder();
  private final String prefix;

  public AttribBufFormatter(String prefix) {
    this.prefix = prefix;
  }

  public AttribBufFormatter() {
    this("");
  }

  @Override
  public void startGroup(String text) {
    startLine();
    buf.append(text).append(":\n");
    startGroup();
  }

  @Override
  public void attrib(String label, String value) {
    startLine();
    buf
      .append(label)
      .append(": ")
      .append(value == null ? "null" : value.toString())
      .append("\n");
  }

  private void startLine() {
    buf.append(prefix);
    for (int i = 0; i < indent; i++) {
      buf.append("  ");
    }
  }

  @Override
  public String toString() { return buf.toString(); }

  @Override
  public void line(String line) {
    startLine();
    buf.append(line).append("\n");
  }
}
