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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.util.EventSequence;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Performs analysis of a query and provides access to the
 * internal details to allow probing of this details during
 * testing.
 *
 * Holds per-query state which can include a test-specific
 * set of options. The query can be analyzed in full, or only analyzed up
 * to rewrites. It provides access to normally-hidden temporary objects
 * such as the analyzer, analysis context and so on. Tests can probe
 * these objects to check for conditions of interest.
 */
public class QueryFixture {

  private final AnalysisFixture analysisFixture_;
  private final TQueryCtx queryCtx_;
  private final String stmt_;
  private ParseNode parseResult_;
  private AnalysisContext analysCtx_;
  private StatementBase parsedStmt_;
  private AnalysisResult analysisResult_;

  public QueryFixture(AnalysisFixture analysisFixture,
      TQueryCtx queryCtx, String stmt) {
    this.analysisFixture_ = analysisFixture;
    this.queryCtx_ = queryCtx;
    this.stmt_ = stmt;
  }

  /**
   * Low-level parse.
   */
  public ParseNode parse() {
    SqlScanner input = new SqlScanner(new StringReader(stmt_));
    SqlParser parser = new SqlParser(input);
    parser.setQueryOptions(this.analysisFixture_.queryOptions_);
    try {
      parseResult_ = (ParseNode) parser.parse().value;
    } catch (Exception e) {
      e.printStackTrace();
      fail("\nParser error:\n" + parser.getErrorMsg(stmt_));
    }
    assertNotNull(parseResult_);
    return parseResult_;
  }

  /**
   * Full analysis, including expression rewrites.
   */
  public QueryFixture analyze() throws AnalysisException {
    makeAnalysisContext();

    // Parse the statement
    parsedStmt_ = this.analysisFixture_.frontend_.parse(
        stmt_, analysCtx_.getQueryOptions());

    try {
      analysisResult_ = analysCtx_.analyzeAndAuthorize(parsedStmt_,
          makeTableCache(), analysisFixture_.frontend_.getAuthzChecker());
      Preconditions.checkNotNull(analysisResult_.getStmt());
    } catch (ImpalaException e) {
      fail(e.getMessage());
      // To keep the Java parser happy.
      throw new IllegalStateException(e);
    }
    return this;
  }

  /**
   * Partial analysis that excludes expression rewrites.
   * Also skips authorization: use this only for rewrite
   * testing.
   */
  public QueryFixture analyzeWithoutRewrite() {
    makeAnalysisContext();

    // Parse the statement
    parsedStmt_ = (StatementBase) parse();

    try {
      Analyzer analyzer = analysCtx_.createAnalyzer(makeTableCache());
      parsedStmt_.analyze(analyzer);
    } catch (ImpalaException e) {
      fail(e.getMessage());
      // To keep the Java parser happy.
      throw new IllegalStateException(e);
    }
    return this;
  }

  private void makeAnalysisContext() {
    // Build a query context
    EventSequence timeline = new EventSequence("Frontend Test Timeline");
    analysCtx_ = new AnalysisContext(queryCtx_,
        AuthorizationConfig.createAuthDisabledConfig(), timeline);
  }

  private StmtTableCache makeTableCache() {
    // Analyze 'stmt', expecting it to pass. Asserts in case of analysis error.
    StmtMetadataLoader mdLoader =
       new StmtMetadataLoader(analysisFixture_.frontend_,
           analysCtx_.getQueryCtx().session.database, null);
    try {
      return mdLoader.loadTables(parsedStmt_);
    } catch (InternalException e) {
      fail(e.getMessage());
      // To keep the Java parser happy.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Asserts that a warning is produced.
   */
  public void expectWarning(String expectedWarning) {
    List<String> actualWarnings = analysisResult_.getAnalyzer().getWarnings();
    boolean matchedWarning = false;
    for (String actualWarning: actualWarnings) {
      if (actualWarning.startsWith(expectedWarning)) {
        matchedWarning = true;
        break;
      }
    }
    if (!matchedWarning) {
      fail(String.format("Did not produce expected warning.\n" +
          "Expected warning:\n%s.\nActual warnings:\n%s",
          expectedWarning, Joiner.on("\n").join(actualWarnings)));
    }
    Preconditions.checkNotNull(parsedStmt_);
  }

  public TQueryCtx queryCtx() { return queryCtx_; }
  public String stmt() { return stmt_; }
  public ParseNode parseNode() { return parsedStmt_; }
  public Analyzer analyzer() { return analysisResult_.getAnalyzer(); }
  public SelectStmt selectStmt() {
    return (SelectStmt) parseNode();
  }
}