// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;

/**
 * Test fixture to analyze queries using a defined set of
 * parameters and options. Provides access to the intermediate
 * state created during analysis to allow finer-grain probing
 * during tests.
 *
 * The AnalysisFixture holds overall state: the frontend,
 * user, database and query options.
 *
 * Unlike the functions in FrontEndTestBase, this fixture does
 * not set the option to skip rewrites. Rather, each test can
 * elect to disable them (for testing individual rules) or to
 * leave them enabled (for tests of the overall rewrite process.)
 *
 * Our intent is to rely on this fixture pattern moving forward.
 * See {@link ExprRewriterTest} for example usage.
 */
public class AnalysisFixture {
  final Frontend frontend_;
  private String db_ = Catalog.DEFAULT_DB;
  private String user_ = System.getProperty("user.name");
  TQueryOptions queryOptions_;

  public AnalysisFixture(Frontend frontend) {
    this.frontend_ = frontend;
  }

  public void setDB(String db) {
    this.db_ = db;
  }

  public void setOptions(TQueryOptions queryOptions) {
    this.queryOptions_ = queryOptions;
  }

  public TQueryOptions getOptions() {
    if (queryOptions_ == null) {
      queryOptions_ = new TQueryOptions();
    }
    return queryOptions_;
  }

  public TQueryOptions cloneOptions() {
    return new TQueryOptions(getOptions());
  }

  /**
   * Disable the optional expression rewrites.
   */
  public void disableExprRewrite() {
    getOptions().setEnable_expr_rewrites(false);
  }

  public TQueryCtx queryContext() {
    return TestUtils.createQueryContext(db_, user_, getOptions());
  }

  /**
   * Analyze a query and return a {@link QueryFixture} for it,
   * using the options defined in this fixture.
   */
  public QueryFixture query(String stmt) throws AnalysisException {
    return new QueryFixture(this, queryContext(), stmt);
  }

  /**
   * Analyze a query and return a {@link QueryFixture} for it,
   * using the specified options.
   */
  public QueryFixture query(TQueryOptions options, String stmt)
      throws AnalysisException {
    return new QueryFixture(
        this, TestUtils.createQueryContext(db_, user_, options), stmt);
  }

  public RewriteFixture rewriteFixture() {
    return new RewriteFixture(this);
  }

  public RewriteFixture rewriteFixture(TQueryOptions options) {
    return new RewriteFixture(this, options);
  }
}
