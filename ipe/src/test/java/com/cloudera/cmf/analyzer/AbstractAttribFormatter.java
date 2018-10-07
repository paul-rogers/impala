package com.cloudera.cmf.analyzer;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

public abstract class AbstractAttribFormatter implements AttribFormatter {

  protected int indent;

  @Override
  public void startGroup() {
    indent++;
  }

  @Override
  public void endGroup() {
    indent--;
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
  public void usAttrib(String label, double value) {
    attrib(label, ParseUtils.toSecs(value));
  }

  @Override
  public void attrib(String label, double value) {
    attrib(label, ParseUtils.format(value));
  }

  @Override
  public <T> void list(String label, Collection<T> values) {
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
