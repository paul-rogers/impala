package org.apache.impala.common.serialize;

public interface JsonSerializable {
  void serialize(ObjectSerializer os);
}
