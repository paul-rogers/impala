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

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TStringLiteral;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

public class StringLiteral extends LiteralExpr {
  private final String value_;

  // Indicates whether this value needs to be unescaped in toThrift().
  private final boolean needsUnescaping_;

  public StringLiteral(String value) {
    this(value, ScalarType.STRING, true);
  }

  public StringLiteral(String value, Type type, boolean needsUnescaping) {
    super(type);
    value_ = value;
    needsUnescaping_ = needsUnescaping;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected StringLiteral(StringLiteral other) {
    super(other);
    value_ = other.value_;
    needsUnescaping_ = other.needsUnescaping_;
  }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    StringLiteral other = (StringLiteral) that;
    return needsUnescaping_ == other.needsUnescaping_ && value_.equals(other.value_);
  }

  @Override
  public int hashCode() { return value_.hashCode(); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return "'" + getNormalizedValue() + "'";
  }

  @VisibleForTesting
  public boolean needsUnescaping() { return needsUnescaping_; }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    String val = (needsUnescaping_) ? getUnescapedValue() : value_;
    msg.string_literal = new TStringLiteral(val);
  }

  /**
   * Returns the original value that the string literal was constructed with,
   * without escaping or unescaping it.
   */
  public String getValueWithOriginalEscapes() { return value_; }

  public String getUnescapedValue() {
    // Unescape string exactly like Hive does. Hive's method assumes
    // quotes so we add them here to reuse Hive's code.
    return BaseSemanticAnalyzer.unescapeSQLString("'" + getNormalizedValue()
        + "'");
  }

  @Override
  public String toString() {
    return "'" + value_ + "':" + type_.toSql();
  }

  /**
   *  String literals can come directly from the SQL of a query or from rewrites like
   * constant folding. So this value normalization to a single-quoted string is
   * necessary because we do not know whether single or double quotes are appropriate.
   *
   *  @return a normalized representation of the string value suitable for embedding in
   *          SQL as a single-quoted string literal.
   */
  @VisibleForTesting
  protected String getNormalizedValue() {
    final int len = value_.length();
    final StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; ++i) {
      final char currentChar = value_.charAt(i);
      if (currentChar == '\\' && (i + 1) < len) {
        final char nextChar = value_.charAt(i + 1);
        // unescape an escaped double quote: remove back-slash in front of the quote.
        if (nextChar == '"' || nextChar == '\'' || nextChar == '\\') {
          if (nextChar != '"') {
            sb.append(currentChar);
          }
          sb.append(nextChar);
          ++i;
          continue;
        }

        sb.append(currentChar);
      } else if (currentChar == '\'') {
        // escape a single quote: add back-slash in front of the quote.
        sb.append("\\\'");
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  @Override
  public String getStringValue() {
    return getValueWithOriginalEscapes();
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .toString();
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    if (targetType.equals(type_)) {
      return this;
    } else if (targetType.isStringType()) {
      type_ = targetType;
      return this;
    } else if (targetType.isNumericType()) {
      return convertToNumber().uncheckedCastTo(targetType);
    } else if (targetType.isDateType()) {
      // Let the BE do the cast so it is in Boost format
      return new CastExpr(targetType, this);
    } else {
      throw new SqlCastException(type_.toSql(), targetType);
    }
  }

  /**
   * Convert this string literal to numeric literal.
   *
   * @return new converted literal (not null)
   *         the type of the literal is determined by the lexical scanner
   * @throws AnalysisException
   *           if NumberFormatException occurs,
   *           or if floating point value is NaN or infinite
   */
  public NumericLiteral convertToNumber()
      throws AnalysisException {
    return NumericLiteral.create(value_);
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    StringLiteral other = (StringLiteral) o;
    return value_.compareTo(other.getStringValue());
  }

  @Override
  public Expr clone() { return new StringLiteral(this); }
}
