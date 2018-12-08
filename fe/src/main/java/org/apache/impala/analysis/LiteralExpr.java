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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TQueryCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Representation of a literal expression. Literals are comparable to allow
 * ordering of HdfsPartitions whose partition-key values are represented as literals.
 */
public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
  private final static Logger LOG = LoggerFactory.getLogger(LiteralExpr.class);

  public LiteralExpr() {
    // Literals start analyzed: there is nothing more to check.
    evalCost_ = LITERAL_COST;
    numDistinctValues_ = 1;
    // Subclass is responsible for setting the type
    analysisDone();
  }

  public LiteralExpr(Type type) {
    this();
    type_ = type;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected LiteralExpr(LiteralExpr other) {
    super(other);
  }

  /**
   * Returns an analyzed literal of 'type'. Returns null for types that do not have a
   * LiteralExpr subclass, e.g. TIMESTAMP.
   */
  public static LiteralExpr create(String value, Type type) throws AnalysisException {
    if (!type.isValid()) {
      throw new UnsupportedFeatureException("Invalid literal type: " + type.toSql());
    }
    LiteralExpr e = null;
    switch (type.getPrimitiveType()) {
      case NULL_TYPE:
        e = new NullLiteral();
        break;
      case BOOLEAN:
        e = new BoolLiteral(value);
        break;
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        e = new NumericLiteral(value, type);
        break;
      case STRING:
      case VARCHAR:
      case CHAR:
        e = new StringLiteral(value);
        break;
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        // TODO: we support TIMESTAMP but no way to specify it in SQL.
        throw new UnsupportedFeatureException("Literal unsupported: " + type.toSql());
      default:
        throw new UnsupportedFeatureException(
            String.format("Literals of type '%s' not supported.", type.toSql()));
    }
    // Need to cast since we cannot infer the type from the value. e.g. value
    // can be parsed as tinyint but we need a bigint.
    return (LiteralExpr) e.uncheckedCastTo(type);
  }

  // NDV set in constructor
  @Override
  protected void computeNumDistinctValues() {

  }

  @Override
  protected void resetAnalysisState() {

  }

  @Override
  public String toString() {
    return getStringValue() + ":" + type_.toSql();
  }

  @Override
  protected float computeEvalCost() {
    return LITERAL_COST;
  }

  /**
   * Returns an analyzed literal from the thrift object.
   */
  public static LiteralExpr fromThrift(TExprNode exprNode, Type colType) {
    try {
      switch (exprNode.node_type) {
        case FLOAT_LITERAL:
          return new NumericLiteral(exprNode.float_literal.value, colType);
        case DECIMAL_LITERAL:
          byte[] bytes = exprNode.decimal_literal.getValue();
          BigDecimal val = new BigDecimal(new BigInteger(bytes));
          ScalarType decimalType = (ScalarType) colType;
          // We store the decimal as the unscaled bytes. Need to adjust for the scale.
          val = val.movePointLeft(decimalType.decimalScale());
          return new NumericLiteral(val, colType);
         case INT_LITERAL:
           return NumericLiteral.create(exprNode.int_literal.value, colType);
        case STRING_LITERAL:
          return StringLiteral.create(exprNode.string_literal.value, colType);
        case BOOL_LITERAL:
          return new BoolLiteral(exprNode.bool_literal.value);
        case NULL_LITERAL:
          return NullLiteral.create(colType);
        default:
          throw new UnsupportedOperationException("Unsupported partition key type: " +
              exprNode.node_type);
      }
    } catch (AnalysisException e) {
      throw new IllegalStateException("Error creating LiteralExpr: ", e);
    }
  }

  /**
   * Returns the string representation of the literal's value. Used when passing
   * literal values to the metastore rather than to Impala backends. This is similar to
   * the toSql() method, but does not perform any formatting of the string values.
   * Neither method unescapes string values.
   */
  public abstract String getStringValue();

  // Swaps the sign of numeric literals.
  // Throws for non-numeric literals.
  public void swapSign() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("swapSign() only implemented for numeric" +
        "literals");
  }

  /**
   * Evaluates the given constant expr and returns its result as a LiteralExpr.
   * Assumes expr has been analyzed. Returns constExpr if is it already a LiteralExpr.
   * Returns null for types that do not have a LiteralExpr subclass, e.g. TIMESTAMP, or
   * in cases where the corresponding LiteralExpr is not able to represent the evaluation
   * result, e.g., NaN or infinity. Returns null if the expr evaluation encountered errors
   * or warnings in the BE.
   * TODO: Support non-scalar types.
   */
  public static LiteralExpr create(Expr constExpr, TQueryCtx queryCtx)
      throws AnalysisException {
    Preconditions.checkState(constExpr.isConstant());
    Preconditions.checkState(constExpr.getType().isValid());
    if (constExpr instanceof LiteralExpr) return (LiteralExpr) constExpr;

    TColumnValue val = null;
    try {
      val = FeSupport.EvalExprWithoutRow(constExpr, queryCtx);
    } catch (InternalException e) {
      LOG.error(String.format("Failed to evaluate expr '%s': %s",
          constExpr.toSql(), e.getMessage()));
      return null;
    }
    return create(val, constExpr.getType());
  }

  public static LiteralExpr create(TColumnValue val, Type type)
      throws AnalysisException {
    switch (type.getPrimitiveType()) {
      case NULL_TYPE:
        return new NullLiteral();
      case BOOLEAN:
        if (val.isSetBool_val()) return new BoolLiteral(val.bool_val);
        break;
      case TINYINT:
        if (val.isSetByte_val()) return NumericLiteral.create(val.byte_val);
        break;
      case SMALLINT:
        if (val.isSetShort_val()) return NumericLiteral.create(val.short_val);
        break;
      case INT:
        if (val.isSetInt_val()) return NumericLiteral.create(val.int_val);
        break;
      case BIGINT:
        if (val.isSetLong_val()) return NumericLiteral.create(val.long_val);
        break;
      case FLOAT:
      case DOUBLE:
        if (val.isSetDouble_val()) {
          // Create using double directly, at extreme ranges the BigDecimal
          // value overflows a double due to conversion issues.
          // A NumericLiteral cannot represent NaN, infinity or negative zero.
          // SqlCastException thrown for these cases.
          try {
            return new NumericLiteral(val.double_val, type);
          } catch (SqlCastException e) {
            return null;
          }
        }
        break;
      case DECIMAL:
        if (val.isSetString_val()) {
          return new NumericLiteral(val.string_val, type);
        }
        break;
      case STRING:
      case VARCHAR:
      case CHAR:
        if (val.isSetBinary_val()) {
          byte[] bytes = new byte[val.binary_val.remaining()];
          val.binary_val.get(bytes);
          // Converting strings between the BE/FE does not work properly for the
          // extended ASCII characters above 127. Bail in such cases to avoid
          // producing incorrect results.
          for (byte b: bytes) if (b < 0) return null;
          try {
            // US-ASCII is 7-bit ASCII.
            String strVal = new String(bytes, "US-ASCII");
            // The evaluation result is a raw string that must not be unescaped.
            return new StringLiteral(strVal, type, false);
          } catch (UnsupportedEncodingException e) {
            return null;
          }
        }
        break;
      case TIMESTAMP:
        // Expects both the binary and string fields to be set, so we get the raw
        // representation as well as the string representation.
        if (val.isSetBinary_val() && val.isSetString_val()) {
          return new TimestampLiteral(val.getBinary_val(), val.getString_val());
        }
        break;
      //case DATE:
      //case DATETIME:
      //  return null;
      default:
        throw new UnsupportedFeatureException(
            String.format("Literals of type '%s' not supported.",
                type.toSql()));
    }
    // None of the fields in the thrift struct were set indicating a NULL.
    return new NullLiteral(type);
  }

  // Order NullLiterals based on the SQL ORDER BY default behavior: NULLS LAST.
  @Override
  public int compareTo(LiteralExpr other) {
    if (Expr.IS_NULL_LITERAL.apply(other)) return 1;
    return Integer.compare(getClass().hashCode(), other.getClass().hashCode());
  }
}
