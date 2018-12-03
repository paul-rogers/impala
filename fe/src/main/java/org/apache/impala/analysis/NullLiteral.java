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
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * A typed NULL literal
 */
public class NullLiteral extends LiteralExpr {

  public NullLiteral() {
    this(Type.NULL);
  }

  public NullLiteral(Type type) {
    super(type);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NullLiteral(NullLiteral other) {
    super(other);
  }

  /**
   * Returns an analyzed NullLiteral of the specified type.
   */
  public static NullLiteral create(Type type) {
    return new NullLiteral(type);
  }

  @Override
  public int hashCode() { return type_.hashCode(); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return getStringValue();
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this).addValue(super.debugString()).toString();
  }

  @Override
  public String getStringValue() { return "NULL"; }

  @Override
  protected Expr uncheckedCastTo(Type targetType) {
    Preconditions.checkState(targetType.isValid());
    type_ = targetType;
    return this;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.NULL_LITERAL;
  }

  @Override
  public Expr clone() { return new NullLiteral(this); }

  @Override
  protected void resetAnalysisState() {
    super.resetAnalysisState();
    type_ = Type.NULL;
  }

  // Order NullLiterals based on the SQL ORDER BY default behavior: NULLS LAST.
  @Override
  public int compareTo(LiteralExpr other) {
    if (!Expr.IS_NULL_LITERAL.apply(other)) return -1;
    return type_.compareTo(((NullLiteral) other).type_);
  }
}
