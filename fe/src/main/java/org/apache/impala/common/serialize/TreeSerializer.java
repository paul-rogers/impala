package org.apache.impala.common.serialize;

public interface TreeSerializer {
  ToJsonOptions options();
  ObjectSerializer root();
  void close();
}
