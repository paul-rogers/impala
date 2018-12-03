package org.apache.impala.common.serialize;

public interface ArraySerializer {
  void value(String value);
  void value(long value);
  ObjectSerializer object();
}