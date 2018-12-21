package org.apache.impala.catalog.facade;

/**
 * The view of the catalog as seen by the planner. Provides a
 * consistent point-in-time semantics for a specific query plan
 * session.
 *
 * Must hide implementation details from the planner such as caching,
 * local vs. remote catalog and so on. For instance, the actual catalog
 * may be backed by a pure in-memory version for testing.
 */
public interface CatalogFacade {
  CatalogSession startSession();
}
