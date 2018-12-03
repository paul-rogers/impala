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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

public interface SelectListItem {
  public static class SelectExpr extends AbstractExpression implements SelectListItem {
    private Expr expr_;
    private String alias_;

    public SelectExpr(Expr expr, String alias) {
      // Called both for "pristine" items created by the parser, and for "synthetic"
      // items for rewriten SELECT statements in the analyzer. Only worth keeping a
      // source copy for unanalyzed, "pristine" expressions.
      super(expr.isAnalyzed() ? null : expr.clone());
      Preconditions.checkNotNull(expr);
      // The source is cloned, not the working copy so that rewritten SELECT
      // works correctly.
      expr_ = expr;
      alias_ = alias;
    }

    /**
     * Constructor for cloning: preserves the original source expression
     * @param expr
     * @param alias
     */
    public SelectExpr(SelectExpr from) {
      super(from.source_);
      expr_ = from.expr_.clone();
      alias_ = from.alias_;
    }

    @Override
    public Expr getExpr() { return expr_; }
    public String getAlias() { return alias_; }
    public void setExpr(Expr expr) { expr_ = expr; }
    @Override
    public boolean isStar() { return false; }

    @Override
    protected Expr prepare() {
      return expr_;
    }

    @Override
    public String toString() {
      Preconditions.checkNotNull(expr_);
      return expr_.toSql() + ((alias_ != null) ? " " + alias_ : "");
    }

    @Override
    public String toSql() { return toSql(DEFAULT); }

    @Override
    public String toSql(ToSqlOptions options) {
      Preconditions.checkNotNull(expr_);
      // Enclose aliases in quotes if Hive cannot parse them without quotes.
      // This is needed for view compatibility between Impala and Hive.
      StringBuilder buf = new StringBuilder();
      buf.append(options == ToSqlOptions.DEFAULT && source_ != null
          ? source_.toSql() : expr_.toSql(options));
      if (alias_ != null) {
        buf.append(" ").append(ToSqlUtils.getIdentSql(alias_));
      }
      return buf.toString();
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
      return new SelectExpr(this);
    }
  }

  public static class SelectWildcard implements SelectListItem {
    // for "[path.]*" (excludes trailing '*')
    private final List<String> rawPath_;

    private SelectWildcard(List<String> path) {
      rawPath_ = path;
    }

    @Override
    public boolean isStar() { return true; }
    public List<String> getRawPath() { return rawPath_; }

    /**
     * Get the expression for the item. Applies only to expressions. Retained
     * in the base class to minimize code changes.
     * @return
     */
    @Override
    public Expr getExpr() {
      throw new IllegalStateException("Tried to get an exception for a wildcard");
    }

    @Override
    public String toString() {
      if (rawPath_ != null) {
        return Joiner.on(".").join(rawPath_) + ".*";
      } else {
        return "*";
      }
    }

    @Override
    public String toSql() { return toSql(DEFAULT); }

    @Override
    public String toSql(ToSqlOptions options) {
      if (rawPath_ != null) {
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

    @Override
    public SelectListItem clone() {
      return new SelectWildcard(rawPath_);
    }
  }

  /**
   * Get the expression for the item. Applies only to expressions. Retained
   * in the base class to minimize code changes.
   * @return
   */
  Expr getExpr();

  boolean isStar();

  String toSql();

  String toSql(ToSqlOptions options);

  SelectListItem clone();

  // select list item corresponding to path_to_struct.*
  static public SelectListItem createWildcard(List<String> rawPath) {
    return new SelectWildcard(rawPath);
  }

  static public SelectListItem createColumn(Expr expr, String alias) {
    return new SelectExpr(expr, alias);
  }

  public static SelectWildcard asWildcard(SelectListItem item) { return (SelectWildcard) item; }
  public static SelectExpr asExpr(SelectListItem item) { return (SelectExpr) item; }
}
