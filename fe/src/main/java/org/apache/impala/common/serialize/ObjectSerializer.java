package org.apache.impala.common.serialize;

public interface ObjectSerializer {
  ToJsonOptions options();
  void field(String name, String value);
  void field(String name, long value);
  void field(String name, double value);
  ObjectSerializer childObject(String name);
}