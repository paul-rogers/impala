package org.apache.impala.common.serialize;

public interface ArraySerializer {
  ToJsonOptions options();
  void value(String value);
  void value(long value);
  void scalar(Object value);
  ObjectSerializer object();
  void object(JsonSerializable obj);
}