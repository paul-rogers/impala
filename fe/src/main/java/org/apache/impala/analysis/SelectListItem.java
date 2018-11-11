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

import java.util.List;

import org.apache.impala.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

class SelectListItem {
  // Original expression, before rewrites
  private String originalExpr_;
  // Effective, rewritten expression
  private Expr expr_;
  private String alias_;

  // for "[path.]*" (excludes trailing '*')
  private final List<String> rawPath_;
  private final boolean isStar_;

  public SelectListItem(Expr expr, String alias) {
    super();
    Preconditions.checkNotNull(expr);
    expr_ = expr;
    alias_ = alias;
    isStar_ = false;
    rawPath_ = null;
  }

  // select list item corresponding to path_to_struct.*
  static public SelectListItem createStarItem(List<String> rawPath) {
    return new SelectListItem(rawPath);
  }

  private SelectListItem(List<String> path) {
    super();
    expr_ = null;
    isStar_ = true;
    rawPath_ = path;
    originalExpr_ = null;
  }

  public Expr getExpr() { return expr_; }
  public String getOriginalExpr() { return originalExpr_; }
  public void setExpr(Expr expr) { expr_ = expr; }
  public boolean isStar() { return isStar_; }
  public String getAlias() { return alias_; }
  public List<String> getRawPath() { return rawPath_; }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    expr_.analyze(analyzer);
    originalExpr_ = expr_.toSql();
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("[SelectItem expr=")
        .append(toSql(true))
        .append(", original=")
        .append(originalExpr_)
        .append("]")
        .toString();
  }

  public String toSql() {
    return toSql(true);
  }

  public String toSql(boolean rewritten) {
    if (!isStar_) {
      String exprStr;
      if (! rewritten && originalExpr_ != null) {
        exprStr = originalExpr_;
      } else {
        Preconditions.checkNotNull(expr_);
        // Enclose aliases in quotes if Hive cannot parse them without quotes.
        // This is needed for view compatibility between Impala and Hive.
        exprStr = expr_.toSql();
      }
      if (alias_ != null) {
        exprStr += " AS " + ToSqlUtils.getIdentSql(alias_);
      }
      return exprStr;
    } else if (rawPath_ != null) {
      Preconditions.checkState(isStar_);
      StringBuilder result = new StringBuilder();
      for (String p: rawPath_) {
        if (result.length() > 0) result.append(".");
        result.append(ToSqlUtils.getIdentSql(p.toLowerCase()));
      }
      result.append(".*");
      return result.toString();
    } else {
      return "*";
    }
  }

  /**
   * Returns a column label for this select list item.
   * If an alias was given, then the column label is the lower case alias.
   * If expr is a SlotRef then directly use its lower case column name.
   * Otherwise, the label is the lower case toSql() of expr or a Hive auto-generated
   * column name (depending on useHiveColLabels).
   * Hive's auto-generated column labels have a "_c" prefix and a select-list pos suffix,
   * e.g., "_c0", "_c1", "_c2", etc.
   *
   * Using auto-generated columns that are consistent with Hive is important
   * for view compatibility between Impala and Hive.
   */
  public String toColumnLabel(int selectListPos, boolean useHiveColLabels) {
    if (alias_ != null) return alias_.toLowerCase();
    if (expr_ instanceof SlotRef) {
      SlotRef slotRef = (SlotRef) expr_;
      return Joiner.on(".").join(slotRef.getResolvedPath().getRawPath());
    }
    // Optionally return auto-generated column label.
    if (useHiveColLabels) return "_c" + selectListPos;
    // Abbreviate the toSql() for analytic exprs.
    if (expr_ instanceof AnalyticExpr) {
      AnalyticExpr expr = (AnalyticExpr) expr_;
      return expr.getFnCall().toSql() + " OVER(...)";
    }
    return expr_.toSql().toLowerCase();
  }

  @Override
  public SelectListItem clone() {
    if (isStar_) return createStarItem(rawPath_);
    return new SelectListItem(expr_.clone(), alias_);
  }

}
