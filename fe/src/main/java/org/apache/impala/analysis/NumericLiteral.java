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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.thrift.TDecimalLiteral;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TFloatLiteral;
import org.apache.impala.thrift.TIntLiteral;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Literal for all numeric values, including integer, floating-point and decimal types.
 * Analysis of this expr determines the smallest type that can hold this value.
 */
public class NumericLiteral extends LiteralExpr {
  // Use the java BigDecimal (arbitrary scale/precision) to represent the value.
  // This object has notions of precision and scale but they do *not* match what
  // we need. BigDecimal's precision is similar to significant figures and scale
  // is the exponent.
  // ".1" could be represented with an unscaled value = 1 and scale = 1 or
  // unscaled value = 100 and scale = 3. Manipulating the value_ (e.g. multiplying
  // it by 10) does not unnecessarily change the unscaled value. Special care
  // needs to be taken when converting between the big decimals unscaled value
  // and ours. (See getUnscaledValue()).
  // A BigDecimal cannot represent special float values like NaN, infinity, or
  // negative zero.
  private BigDecimal value_;

  // If true, this literal has been explicitly cast to a type and should not
  // be analyzed (which infers the type from value_).
  private boolean explicitlyCast_;

  public NumericLiteral(BigDecimal value) throws InvalidValueException {
    value_ = value;
    type_ = inferType(value);
  }

  public NumericLiteral(String value, Type t) throws InvalidValueException {
    Preconditions.checkArgument(t.isScalarType());
    ScalarType scalarType = (ScalarType) t;
    try {
      value_ = new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new InvalidValueException("Invalid numeric literal: " + value, e);
    }
    type_ = NumericLiteral.inferType(value_);
    if (type_.isDecimal() && t.isDecimal()) {
      // Verify that the input decimal value is consistent with the specified
      // column type.
      if (!scalarType.isSupertypeOf((ScalarType) type_)) {
        StringBuilder errMsg = new StringBuilder();
        errMsg.append("Invalid ").append(t);
        errMsg.append(" value: " + value);
        throw new InvalidValueException(errMsg.toString());
      }
    }
    else if (type_.isIntegerType() && scalarType.isIntegerType()) {
      if (!ScalarType.isAssignable(scalarType, (ScalarType) type_)) {
        throw new InvalidValueException("Value " + value +
            " is too large for type " + t.toSql());
      }
    }
    else if (t.isFloatingPointType()) explicitlyCastToFloat(t);
  }

  /**
   * The versions of the ctor that take types assume the type is correct
   * and the NumericLiteral is created as analyzed with that type. The specified
   * type is preserved across substitutions and re-analysis.
   */
  public NumericLiteral(BigInteger value, Type type) {
    value_ = new BigDecimal(value);
    type_ = type;
    explicitlyCast_ = true;
  }

  public NumericLiteral(BigDecimal value, Type type) {
    value_ = value;
    type_ = type;
    explicitlyCast_ = true;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NumericLiteral(NumericLiteral other) {
    super(other);
    value_ = other.value_;
    explicitlyCast_ = other.explicitlyCast_;
  }

  public static NumericLiteral create(int value) {
    try {
    return new NumericLiteral(new BigDecimal(value));
    } catch (AnalysisException e) {
      // Should never occur for int values
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns true if 'v' can be represented by a NumericLiteral, false otherwise.
   * Special float values like NaN, infinity, and negative zero cannot be represented
   * by a NumericLiteral.
   */
  public static boolean isValidLiteral(double v) {
    if (Double.isNaN(v) || Double.isInfinite(v)) return false;
    // Check for negative zero.
    if (v == 0 && 1.0 / v == Double.NEGATIVE_INFINITY) return false;
    return true;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .add("type", type_)
        .toString();
  }

  @Override
  public String toString() {
    return value_.toString() + ":" + type_.toSql();
  }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;

    NumericLiteral tmp = (NumericLiteral) that;
    if (!tmp.value_.equals(value_)) return false;
    // Analyzed Numeric literals of different types are distinct.
    if ((isAnalyzed() && tmp.isAnalyzed()) && (!getType().equals(tmp.getType()))) return false;
    return true;
  }

  @Override
  public int hashCode() { return value_.hashCode(); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (options.showImplictCasts()) {
      return "CAST(" + getStringValue() + " AS " + type_.toSql() + ")";
    }
    return getStringValue();
  }

  @VisibleForTesting
  public boolean isExplicitCast() { return explicitlyCast_; }

  @Override
  public String getStringValue() {
    // BigDecimal returns CAST(0, DECIMAL(38, 38))
    // as 0E-38. We want just 0.
    return value_.compareTo(BigDecimal.ZERO) == 0
        ? "0" : value_.toString();
  }

  public double getDoubleValue() { return value_.doubleValue(); }
  public long getLongValue() { return value_.longValue(); }
  public long getIntValue() { return value_.intValue(); }

  @Override
  protected void toThrift(TExprNode msg) {
    switch (type_.getPrimitiveType()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(value_.longValue());
        break;
      case FLOAT:
      case DOUBLE:
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(value_.doubleValue());
        break;
      case DECIMAL:
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        TDecimalLiteral literal = new TDecimalLiteral();
        literal.setValue(getUnscaledValue().toByteArray());
        msg.decimal_literal = literal;
        break;
      default:
        Preconditions.checkState(false);
    }
  }

  public BigDecimal getValue() { return value_; }

  public static void resetType(Expr expr) {
    if (expr instanceof NumericLiteral) {
      ((NumericLiteral) expr).inferType();
    }
  }

  // Temporary implementation to work around the current
  // double-analyze step
  @Override
  public Expr reset() {
    inferType();
    return this;
  }

  public void inferType() {
    if (explicitlyCast_) return;
    try {
      type_ = inferType(value_);
    } catch (AnalysisException e) {
      // Should not occur, we are just reinterpreting
      // a valid value
      throw new IllegalStateException(e);
    }
  }


  public static ScalarType inferType(BigDecimal value) throws InvalidValueException {
    // Compute the precision and scale from the BigDecimal.
    Type type = TypesUtil.computeDecimalType(value);
    if (type == null) {
      // Literal could not be stored in any of the supported decimal precisions and
      // scale. Store it as a float/double instead.
      double d = value.doubleValue();
      if (Double.isInfinite(d)) {
        throw new InvalidValueException("Numeric literal '" + value.toString() +
              "' exceeds maximum range of doubles.");
      } else if (d == 0 && value != BigDecimal.ZERO) {
        // Note: according to the SQL standard, section 4.4, this
        // should simply truncate the lower digits, returning 0.
        throw new InvalidValueException("Numeric literal '" + value.toString() +
              "' underflows minimum resolution of doubles.");
      }

      if (value.floatValue() == d) {
        return Type.FLOAT;
      } else {
        return Type.DOUBLE;
      }
    }

    // The value is a valid Decimal. Prefer an integer type.
    Preconditions.checkState(type.isScalarType());
    ScalarType scalarType = (ScalarType) type;
    if (scalarType.decimalScale() != 0) return scalarType;
    if (value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) <= 0 &&
        value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) >= 0) {
      return Type.TINYINT;
    } else if (value.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) <= 0 &&
               value.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) >= 0) {
      return Type.SMALLINT;
    } else if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0 &&
               value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
      return Type.INT;
    } else if (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 &&
               value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
      return Type.BIGINT;
    }
    // Value is too large for BIGINT, keep decimal.
    return scalarType;
  }

  /**
   * Explicitly cast this literal to 'targetType'. The targetType must be a
   * float point type.
   */
  protected void explicitlyCastToFloat(Type targetType) {
    Preconditions.checkState(targetType.isFloatingPointType());
    type_ = targetType;
    explicitlyCast_ = true;
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    // Implicit casting to decimals allows truncating digits from the left of the
    // decimal point (see TypesUtil). A literal that is implicitly cast to a decimal
    // with truncation is wrapped into a CastExpr so the BE can evaluate it and report
    // a warning. This behavior is consistent with casting/overflow of non-constant
    // exprs that return decimal.
    // IMPALA-1837: Without the CastExpr wrapping, such literals can exceed the max
    // expected byte size sent to the BE in toThrift().
    if (targetType.isDecimal()) {
      ScalarType decimalType = (ScalarType) targetType;
      // analyze() ensures that value_ never exceeds the maximum scale and precision.
      Preconditions.checkState(isAnalyzed());
      // Sanity check that our implicit casting does not allow a reduced precision or
      // truncating values from the right of the decimal point.
     if (value_.precision() > decimalType.decimalPrecision() ||
         value_.scale() > decimalType.decimalScale()) {
       throw new SqlCastException("Value " + value_.toString() +
             " cannot be cast to type " + decimalType.toSql());
      }
      int valLeftDigits = value_.precision() - value_.scale();
      int typeLeftDigits = decimalType.decimalPrecision() - decimalType.decimalScale();
      if (typeLeftDigits < valLeftDigits) return new CastExpr(targetType, this);
    }
    type_ = targetType;
    return this;
  }

  @Override
  public void swapSign() {
    value_ = value_.negate();

    // Swapping the sign may change the type:
    // 128 is a SMALLINT, -128 is a TINYINT
    inferType();
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    NumericLiteral other = (NumericLiteral) o;
    return value_.compareTo(other.value_);
  }

  // Returns the unscaled value of this literal. BigDecimal doesn't treat scale
  // the way we do. We need to pad it out with zeros or truncate as necessary.
  private BigInteger getUnscaledValue() {
    Preconditions.checkState(type_.isDecimal());
    BigInteger result = value_.unscaledValue();
    int valueScale = value_.scale();
    // If valueScale is less than 0, it indicates the power of 10 to multiply the
    // unscaled value. This path also handles this case by padding with zeros.
    // e.g. unscaled value = 123, value scale = -2 means 12300.
    ScalarType decimalType = (ScalarType) type_;
    return result.multiply(BigInteger.TEN.pow(decimalType.decimalScale() - valueScale));
  }

  @Override
  public Expr clone() { return new NumericLiteral(this); }

  /**
   * Check overflow.
   */
  public static boolean isOverflow(BigDecimal value, Type type)
      throws AnalysisException {
    switch (type.getPrimitiveType()) {
      case TINYINT:
        return (value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0);
      case SMALLINT:
        return (value.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0);
      case INT:
        return (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0);
      case BIGINT:
        return (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0);
      case FLOAT:
        return (value.compareTo(BigDecimal.valueOf(Float.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Float.MIN_VALUE)) < 0);
      case DOUBLE:
        return (value.compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) > 0 ||
            value.compareTo(BigDecimal.valueOf(Double.MIN_VALUE)) < 0);
      case DECIMAL:
        return (TypesUtil.computeDecimalType(value) == null);
      default:
        throw new AnalysisException("Overflow check on " + type.toSql() +
            " isn't supported.");
    }
  }
}
