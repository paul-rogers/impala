package com.cloudera.cmf.analyzer;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

public class AttribBufFormatter implements AttribFormatter {

  private final StringBuilder buf = new StringBuilder();
  private final String prefix;
  private int indent;

  public AttribBufFormatter(String prefix) {
    this.prefix = prefix;
  }

  public AttribBufFormatter() {
    this("");
  }

  @Override
  public void startGroup() {
    indent++;
  }

  @Override
  public void startGroup(String text) {
    startLine();
    buf.append(text).append(":\n");
    startGroup();
  }

  @Override
  public void endGroup() {
    indent--;
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

  @Override
  public void attrib(String label, double value) {
    startLine();
    buf
      .append(label)
      .append(": ")
      .append(ParseUtils.format(value))
      .append("\n");
  }

  @Override
  public void list(String label, String[] values) {
    if (values == null || values.length == 0) {
      attrib(label, "None");
      return;
    }
    startGroup(label);
    for (String value : values) {
      line(value);
    }
    endGroup();
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
  public void attrib(String label, long value) {
    attrib(label, (double) value);
  }

  @Override
  public void attrib(String label, int value) {
    attrib(label, (double) value);
  }

  @Override
  public void attrib(String label, int[] value) {
    attrib(label, Joiner.on(", ").join(Ints.asList(value)));
  }

  @Override
  public void line(String line) {
    startLine();
    buf.append(line).append("\n");
  }

  @Override
  public void usAttrib(String label, double value) {
    attrib(label, ParseUtils.toSecs(value));
  }

  @Override
  public <T> void list(String label, List<T> values) {
    if (values == null || values.isEmpty()) {
      attrib(label, "None");
      return;
    }
    startGroup(label);
    for (T value : values) {
      line(value.toString());
    }
    endGroup();
  }

}
