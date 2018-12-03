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

import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.thrift.TBoolLiteral;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.Objects;

public class BoolLiteral extends LiteralExpr {
  private final boolean value_;

  public BoolLiteral(boolean value) {
    super(Type.BOOLEAN);
    this.value_ = value;
  }

  public BoolLiteral(String value) throws AnalysisException {
    this(parseBoolean(value));
  }

  public static boolean parseBoolean(String value) throws InvalidValueException {
    if (value.equalsIgnoreCase("true")) {
      return true;
    } else if (value.equalsIgnoreCase("false")) {
      return false;
    } else {
      // Unlike Java, SQL requires an exact match
      throw new InvalidValueException("invalid BOOLEAN literal: " + value);
    }
  }

  /**
   * Copy c'tor used in clone.
   */
  protected BoolLiteral(BoolLiteral other) {
    super(other);
    value_ = other.value_;
  }

  public static BoolLiteral create(boolean value) {
    return new BoolLiteral(value);
  }

  public static BoolLiteral create(String value) throws AnalysisException {
    return new BoolLiteral(value);
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .toString();
  }

  @Override
  public boolean localEquals(Expr that) {
    return super.localEquals(that) && ((BoolLiteral) that).value_ == value_;
  }

  @Override
  public int hashCode() { return value_ ? 1 : 0; }

  public boolean getValue() { return value_; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return getStringValue();
  }

  @Override
  public String getStringValue() {
    return value_ ? "TRUE" : "FALSE";
  }

  // No need for type for a BOOLEAN
  @Override
  public String toString() { return getStringValue(); }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BOOL_LITERAL;
    msg.bool_literal = new TBoolLiteral(value_);
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    if (targetType.equals(this.type_)) {
      return this;
    } else {
      return new CastExpr(targetType, this);
    }
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    BoolLiteral other = (BoolLiteral) o;
    if (value_ == other.getValue()) return 0;
    return value_ ? 1 : -1;
  }

  @Override
  public Expr clone() { return new BoolLiteral(this); }
}
