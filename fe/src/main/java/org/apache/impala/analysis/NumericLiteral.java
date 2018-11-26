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

  public static final BigDecimal MIN_TINYINT = BigDecimal.valueOf(Byte.MIN_VALUE);
  public static final BigDecimal MAX_TINYINT = BigDecimal.valueOf(Byte.MAX_VALUE);
  public static final BigDecimal MIN_SMALLINT = BigDecimal.valueOf(Short.MIN_VALUE);
  public static final BigDecimal MAX_SMALLINT = BigDecimal.valueOf(Short.MAX_VALUE);
  public static final BigDecimal MIN_INT = BigDecimal.valueOf(Integer.MIN_VALUE);
  public static final BigDecimal MAX_INT = BigDecimal.valueOf(Integer.MAX_VALUE);
  public static final BigDecimal MIN_BIGINT = BigDecimal.valueOf(Long.MIN_VALUE);
  public static final BigDecimal MAX_BIGINT = BigDecimal.valueOf(Long.MAX_VALUE);
  public static final BigDecimal MAX_FLOAT = BigDecimal.valueOf(Float.MAX_VALUE);
  public static final BigDecimal MIN_FLOAT = MAX_FLOAT.negate();
  public static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);
  public static final BigDecimal MIN_DOUBLE = MAX_DOUBLE.negate();

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

  public NumericLiteral(BigDecimal value) throws SqlCastException {
    value_ = value;
    type_ = inferType(value);
    analysisDone();
  }

  public NumericLiteral(String value, Type t) throws SqlCastException {
    this(new BigDecimal(value), t);
  }

  /**
   * The versions of the ctor that take types assume the type is correct
   * and the NumericLiteral is created as analyzed with that type. The specified
   * type is preserved across substitutions and re-analysis.
   */
  public NumericLiteral(BigInteger value, Type type) throws SqlCastException {
    this(new BigDecimal(value), type);
  }

  public NumericLiteral(BigDecimal value, Type type) throws SqlCastException {
    value_ = convertValue(value, type);
    type_ = type;
    explicitlyCast_ = true;
    analysisDone();
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NumericLiteral(NumericLiteral other) {
    super(other);
    value_ = other.value_;
    explicitlyCast_ = other.explicitlyCast_;
  }

  public static NumericLiteral create(BigDecimal value) {
    try {
     return new NumericLiteral(value);
    } catch (SqlCastException e) {
      // Should never occur for int values
      throw new IllegalStateException(e);
    }
  }

  public static NumericLiteral create(BigDecimal value, Type type) {
    try {
      return new NumericLiteral(value, type);
    } catch (AnalysisException e) {
      // Should never occur for int values
      throw new IllegalStateException(e);
    }
  }

  public static NumericLiteral create(int value) {
    return create(new BigDecimal(value));
  }

  public static NumericLiteral create(int value, Type type) {
    return create(new BigDecimal(value), type);
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
  public int getIntValue() { return value_.intValue(); }

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

  public static boolean isBetween(BigDecimal value, BigDecimal low, BigDecimal high) {
    return value.compareTo(low) >= 0 &&
           value.compareTo(high) <= 0;
  }

  public static boolean isTinyInt(BigDecimal value) {
    return isBetween(value, MIN_TINYINT, MAX_TINYINT);
  }

  public static boolean isSmallInt(BigDecimal value) {
    return isBetween(value, MIN_SMALLINT, MAX_SMALLINT);
  }

  public static boolean isInt(BigDecimal value) {
    return isBetween(value, MIN_INT, MAX_INT);
  }

  public static boolean isBigInt(BigDecimal value) {
    return isBetween(value, MIN_BIGINT, MAX_BIGINT);
  }

  public static boolean isFloat(BigDecimal value) {
    return isBetween(value, MIN_FLOAT, MAX_FLOAT);
  }

  public static boolean isDouble(BigDecimal value) {
    return isBetween(value, MIN_DOUBLE, MAX_DOUBLE);
  }

  public static boolean isDecimal(BigDecimal value) {
    return TypesUtil.computeDecimalType(value) != null;
  }

  public static ScalarType inferType(BigDecimal value) throws SqlCastException {
    // Compute the precision and scale from the BigDecimal.
    Type type = TypesUtil.computeDecimalType(value);
    if (type == null) {
      // Literal could not be stored in any of the supported decimal precisions and
      // scale. Store it as a float/double instead.
      double d = value.doubleValue();
      if (Double.isInfinite(d)) {
        throw new SqlCastException("Numeric literal '" + value.toString() +
              "' exceeds maximum range of DOUBLE.");
      } else if (d == 0 && value != BigDecimal.ZERO) {
        // Note: according to the SQL standard, section 4.4, this
        // should simply truncate the lower digits, returning 0.
        throw new SqlCastException("Numeric literal '" + value.toString() +
              "' underflows minimum resolution of DOUBLE.");
      }

      if (value.floatValue() == d) {
        // Note: this will seldom happen. Float can store up to
        // e38, which is within the range of a BIGINT.
        return Type.FLOAT;
      } else {
        return Type.DOUBLE;
      }
    }

    // The value is a valid Decimal. Prefer an integer type.
    Preconditions.checkState(type.isScalarType());
    ScalarType scalarType = (ScalarType) type;
    if (scalarType.decimalScale() != 0) return scalarType;
    if (isTinyInt(value)) return Type.TINYINT;
    if (isSmallInt(value)) return Type.SMALLINT;
    if (isInt(value)) return Type.INT;
    if (isBigInt(value)) return Type.BIGINT;
    // Value is too large for BIGINT, keep decimal.
    return scalarType;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    if (explicitlyCast_) return;
    type_ = inferType(value_);
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

  /**
   * Convert to the given type, performing range checks.
   * (The method name is a misnomer.) If the cast would result
   * in a change of value (such as rounding for decimal), a new
   * value is returned, else the cast is done in place.
   *
   * Truncates trailing fractional digits as needed to fit
   * the target type (as allowed by the SQL standard, section 4.4)
   */
   public static BigDecimal convertValue(BigDecimal value,
       Type targetType) throws SqlCastException {
    Preconditions.checkState(targetType.isNumericType());
    // Don't allow overflow. Checks only extreme range for DECIMAL.
    if (isOverflow(value, targetType)) {
      throw new SqlCastException( value, targetType);
    }

    // If cast to an integer type, round the fractional part.
    if (targetType.isIntegerType() && value.scale() != 0) {
      return
          value.setScale(0, BigDecimal.ROUND_HALF_UP);
    }

    // If non-decimal, use the existing value.
    if (!targetType.isDecimal()) return value;

    // Check for Decimal overflow.
    ScalarType decimalType = (ScalarType) targetType;
    int valLeftDigits = value.precision() - value.scale();
    int typeLeftDigits = decimalType.decimalPrecision() - decimalType.decimalScale();
    // Special case 0, it is reported as having 1 left digit, while 0.1
    // has zero left digits.
    if (typeLeftDigits < valLeftDigits && value.compareTo(BigDecimal.ZERO) != 0) {
      throw new SqlCastException( value, targetType);
    }

    // Truncate (round) extra digits if necessary.
    if (value.scale() > decimalType.decimalScale()) {
      return value.setScale(decimalType.decimalScale(), BigDecimal.ROUND_HALF_UP);
    }

    // Existing value fits, use it.
    return value;
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    BigDecimal converted = convertValue(value_, targetType);
    if (converted == value_) {
      // Use existing value, cast in place.
      // Revisit: better for literals to be immutable, even in type.
      type_ = targetType;
      return this;
    } else {
      // Value changed, create a new literal.
      return new NumericLiteral(converted, targetType);
    }
  }

  @Override
  public void swapSign() {
    // Swapping the sign may change the type:
    // 128 is a SMALLINT, -128 is a TINYINT
    value_ = value_.negate();
    try {
      type_ = inferType(value_);
    } catch (SqlCastException e) {
      // Should never occur
      throw new IllegalStateException(e);
    }
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
  public static boolean isOverflow(BigDecimal value, Type type) {
    switch (type.getPrimitiveType()) {
      case TINYINT:
        return !isTinyInt(value);
      case SMALLINT:
        return !isSmallInt(value);
      case INT:
        return !isInt(value);
      case BIGINT:
        return !isBigInt(value);
      case FLOAT:
        return !isFloat(value);
      case DOUBLE:
        return !isDouble(value);
      case DECIMAL:
        return !isDecimal(value);
      default:
        throw new IllegalArgumentException("Overflow check on " + type.toSql() +
            " isn't supported.");
    }
  }
}
