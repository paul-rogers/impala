package org.apache.impala.catalog.facade;

public interface DbFacade {
  TableFacade resolveTable(String tableName);
  Iterable<String> listTables();
}
