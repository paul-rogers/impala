package org.apache.impala.catalog.facade;

/**
 * Represents a catalog session (like a transaction) used for planning a
 * single query against a snapshot of the metadata used for that query.
 * This interface has a temporal aspect unique to planning which is not
 * directly related to the caching implementation of the catalog itelf.
 *
 */
public interface CatalogSession {

  /**
   * Resolve (retrieve) a database against the current catalog version.
   * @param dbName
   * @return the database, or null if the name cannot be resolved
   */
  DbFacade resolveDb(String dbName);

  /**
   * Release resources for this session. Called at the (successful
   * or failed) completion of planning.
   */
  void close();
}
