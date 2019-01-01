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

import org.apache.impala.common.AnalysisException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Stub expression rewrite engine. For now, holds only the rewrite mode.
 */
public class ExprAnalyzer {
  public enum RewriteMode {
    NONE,
    REQUIRED,
    OPTIONAL
  }

  private final Analyzer analyzer_;
  private RewriteMode rewriteMode_ = RewriteMode.REQUIRED;

  @VisibleForTesting
  public ExprAnalyzer(Analyzer analyzer, RewriteMode rewriteMode) {
    Preconditions.checkNotNull(analyzer);
    analyzer_ = analyzer;
    rewriteMode_ = rewriteMode;
  }

  public ExprAnalyzer(Analyzer analyzer) {
    this(analyzer,
        analyzer.getQueryCtx().getClient_request().getQuery_options().enable_expr_rewrites
        ? RewriteMode.OPTIONAL : RewriteMode.REQUIRED);
  }

  public Expr analyze(Expr expr) throws AnalysisException {
    expr.analyze(analyzer_);
    return expr;
  }

  public boolean isEnabled(RewriteMode mode) {
    switch (mode) {
    case OPTIONAL:
      return rewriteMode_ == RewriteMode.OPTIONAL;
    case REQUIRED:
      return rewriteMode_ != RewriteMode.NONE;
    default:
      return false;
    }
  }

  @VisibleForTesting
  public void setRewriteMode(RewriteMode mode) {
    rewriteMode_ = mode;
  }

  public Analyzer analyzer() { return analyzer_; }
}
