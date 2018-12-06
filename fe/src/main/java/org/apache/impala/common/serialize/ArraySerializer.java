package org.apache.impala.common.serialize;

public interface ArraySerializer {
  void value(String value);
  void value(long value);
  void value(Object value);
  ObjectSerializer object();
}