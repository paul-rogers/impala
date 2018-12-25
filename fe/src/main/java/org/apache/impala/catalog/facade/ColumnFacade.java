package org.apache.impala.catalog.facade;

import org.apache.impala.catalog.Type;

public interface ColumnFacade {
  int index();
  String name();
  Type type();
  int width();
  int avgWidth();
  int maxWidth();
  int ndv();
  int nullCount();
}
