package com.cloudera.cmf.analyzer;

import java.util.Collection;

public interface AttribFormatter {
  void startGroup(String text);
  void startGroup();
  void endGroup();
  void attrib(String label, String value);
  void attrib(String label, double value);
  void attrib(String label, int value);
  void attrib(String label, long value);
  void attrib(String label, int value[]);
  void usAttrib(String label, double value);
  void line(String line);
  void list(String label, String values[]);
  <T> void list(String label, Collection<T> values);
  boolean verbose();
}
