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

import java.io.IOException;
import java.io.StringReader;
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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java_cup.runtime.Symbol;

/**
 * Literal for all numeric values, including integer, floating-point and decimal
 * types. The constructor determines the "natural type" of this literal:
 * smallest type that can hold this value.
 *
 * A numeric literal has an "explicit" type which starts as the natural type.
 * The explicit type can be widened via a user-supplied cast: CAST(1 AS INT).
 * Constant folding uses the BE to convert such an expression into a new numeric
 * literal with the explicit type set to the requested type.
 *
 * The user can request an invalid cast: CAST(1000 AS TINYINT). In this case the
 * natural type is SMALLINT. When constant folding asks the BE to rewrite the
 * expression, the BE fails. In this case, the CAST is left in the expression
 * tree so that the literal itself never carries a type which is invalid for its
 * value. The BE will handle the invalid cast at runtime. TODO: if the query
 * will fail at runtime, should the FE simply go ahead and fail the query at
 * analysis time>
 *
 * The literal also has an "implicit type" which starts the same as the explicit
 * type. The implicit type changes as a result of casts the analyzer uses to
 * adjust input types for functions and arithmetic operations. Here, the code
 * attempts to "fold" the cast into the literal directly. TODO: Revisit this
 * implicit folding route; now that constant folding is available, perhaps
 * both cases should work the same: by using the constant folding rule and
 * allowing the BE to do the work. This saves trying to keep the FE and BE
 * conversion code in sync.
 *
 * The analyzer makes two analysis passes over the code: analyze, rewrite,
 * analyze again. Implicit types can lead to repeated type widening of
 * expressions. In the first analysis round:
 *
 * 1:TINYINT + 2:TINYINT --> SMALLINT result
 *
 * Cast inputs to the output type:
 *
 * CAST(1:TINYINT AS SMALLINT) + CAST(1:TINYINT AS SMALLINT)
 *
 * In the second round, we must reset the implicit type back to the explicit
 * type, else the type propagation works with the previous implicit type
 * producing:
 *
 * 1:SMALLINT + 1:SMALLINT -> INT result
 *
 * With new implicit casts added:
 *
 * CAST(1:SMALLINT AS INT) + CAST(1:SMALLINT AS INT)
 *
 * TODO: The above problem would disappear if the analyzer is modified to
 * make only one analysis/type propagation pass over each expression.
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

  /**
   * The explicit type of the literal, which must be wider than the "natural"
   * type. Set via the constructor, as the result of pushing an explicit
   * (user-provided) CAST into this literal.
   */
  private Type explicitType_;

  public NumericLiteral(BigDecimal value) throws SqlCastException {
    value_ = value;
    type_ = inferType(value);
    explicitType_ = type_;
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
    super(type);
    value_ = convertValue(value, type);
    explicitType_ = type_;
  }

  /**
   * Constructor for double values. Converting to BigDecimal and then
   * validating the value leads to overflow at the extreme ranges.
   * That is, conversion to a BigDecimal and back is not stable.
   * @throws SqlCastException
   */
  public NumericLiteral(double value, Type type) throws SqlCastException {
    super(type);
    Preconditions.checkArgument(type == Type.DOUBLE || type == Type.FLOAT);
    // Reject INF, NaN
    if (!isDouble(value)) {
      throw new SqlCastException("Value " + Double.toString(value) +
          " cannot be cast to " + type.toSql());
    }
    value_ = new BigDecimal(value);
    // Ensure value is in range of a FLOAT
    if (type == Type.FLOAT && !fitsInFloat(value_)) {
      throw new SqlCastException(value_, type);
    }
    explicitType_ = type_;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NumericLiteral(NumericLiteral other) {
    super(other);
    value_ = other.value_;
    explicitType_ = other.explicitType_;
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

  public static NumericLiteral create(long value) {
    return create(new BigDecimal(value));
  }

  public static NumericLiteral create(long value, Type type) {
    return create(new BigDecimal(value), type);
  }

  /**
   * Create a numeric literal from a SQL-like string.
   * Valid formats:
   *  "123" - simple number
   *  "-123" - negative number
   *  " - 123 " - with spaces
   *  "- -123" - double equals, but only with a space between the
   *             equal signs
   *  "123.45" - decimal number (not float)
   *  "123e45" - double number (too big for decimal)
   *
   * Note that "--10" cannot be supported because that is a SQL
   * comment.
   * @param value string value
   * @return numeric literal
   * @throws AnalysisException for format or range errors
   */
  public static NumericLiteral create(String value) throws AnalysisException {
    StringReader reader = new StringReader(value);
    SqlScanner scanner = new SqlScanner(reader);
    // For distinguishing positive and negative numbers.
    boolean negative = false;
    Symbol sym;
    try {
      // We allow simple chaining of MINUS to recognize negative numbers.
      // Currently we can't handle string literals containing full fledged expressions
      // which are implicitly cast to a numeric literal.
      // This would require invoking the parser.
      // Note: syntax must be - -10, since --10 is a comment
      sym = scanner.next_token();
      while (sym.sym == SqlParserSymbols.SUBTRACT) {
        negative = !negative;
        sym = scanner.next_token();
      }
    } catch (IOException e) {
      // Should never occur
      throw new InvalidValueException("Failed to convert string literal to number: " + value, e);
    }
    if (sym.sym == SqlParserSymbols.NUMERIC_OVERFLOW) {
      throw new SqlCastException("Number too large: " + value);
    }
    if (sym.sym != SqlParserSymbols.INTEGER_LITERAL &&
        sym.sym != SqlParserSymbols.DECIMAL_LITERAL) {
        throw new InvalidValueException("Invalid number: " + value);
    }

    try {
      BigDecimal val = (BigDecimal) sym.value;
      if (negative) val = val.negate();
      return new NumericLiteral(val);
    } catch (NumberFormatException e) {
      // Should never occur
      throw new InvalidValueException("Invalid number format: " + value, e);
    }
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .add("type", type_)
        .toString();
  }

  public Type getExplicitType() { return explicitType_; }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    // Analyzed Numeric literals of different types are distinct.
    if (!getType().equals(that.getType())) { return false; }

    NumericLiteral other = (NumericLiteral) that;
    // Must use compareTo() since equals() won't match if the
    // values have different precisions.
    return other.value_.compareTo(value_) == 0;
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

  // Predicates to determine if the given value fits within the range of
  // the given data type.
  public static boolean fitsInTinyInt(BigDecimal value) {
    return isBetween(value, MIN_TINYINT, MAX_TINYINT);
  }

  public static boolean fitsInSmallInt(BigDecimal value) {
    return isBetween(value, MIN_SMALLINT, MAX_SMALLINT);
  }

  public static boolean fitsInInt(BigDecimal value) {
    return isBetween(value, MIN_INT, MAX_INT);
  }

  public static boolean fitsInBigInt(BigDecimal value) {
    return isBetween(value, MIN_BIGINT, MAX_BIGINT);
  }

  public static boolean fitsInFloat(BigDecimal value) {
    return isBetween(value, MIN_FLOAT, MAX_FLOAT);
  }

  public static boolean fitsInDouble(BigDecimal value) {
    return !Double.isInfinite(value.doubleValue());
  }

  /**
   * Returns true if 'v' can be represented by a NumericLiteral, false otherwise.
   * Special float values like NaN, infinity, and negative zero cannot be represented
   * by a NumericLiteral.
   */
  public static boolean isDouble(double v) {
    if (Double.isNaN(v) || Double.isInfinite(v)) return false;
    // Check for negative zero.
    if (v == 0 && 1.0 / v == Double.NEGATIVE_INFINITY) return false;
    return true;
  }


  public static boolean isDecimal(BigDecimal value) {
    return TypesUtil.computeDecimalType(value) != null;
  }

  /**
   * Infer the natural (smallest) type required to hold the given numeric value.
   *
   * @throws SqlCastException if the value is not valid for any type
   */
  public static ScalarType inferType(BigDecimal value) throws SqlCastException {
    // Compute the precision and scale from the BigDecimal.
    Type type = TypesUtil.computeDecimalType(value);
    if (type == null) {
      // Literal could not be stored in any of the supported decimal precisions and
      // scale. Store it as a float/double instead.
      double d = value.doubleValue();
      if (!fitsInDouble(value)) {
        throw new SqlCastException("Numeric literal '" + value.toString() +
              "' exceeds maximum range of DOUBLE.");
      } else if (d == 0 && value != BigDecimal.ZERO) {
        // Note: according to the SQL standard, section 4.4, this
        // should simply truncate the lower digits, returning 0.
        throw new SqlCastException("Numeric literal '" + value.toString() +
              "' underflows minimum resolution of DOUBLE.");
      }

      // Always return a double. FLOAT does not add much value.
      // FLOAT can store up to 1e38, which is within the range of a DECIMAL.
      return Type.DOUBLE;
    }

    // The value is a valid Decimal. Prefer an integer type.
    Preconditions.checkState(type.isScalarType());
    ScalarType scalarType = (ScalarType) type;
    if (scalarType.decimalScale() != 0) return scalarType;
    if (fitsInTinyInt(value)) return Type.TINYINT;
    if (fitsInSmallInt(value)) return Type.SMALLINT;
    if (fitsInInt(value)) return Type.INT;
    if (fitsInBigInt(value)) return Type.BIGINT;
    // Value is too large for BIGINT, keep decimal.
    return scalarType;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // On re-analysis after rewrite or other operation, revert to the
    // explicit type. Ideally would not be needed, but seems to be
    // required after a clone() operation.
    type_ = explicitType_;
  }

  @Override
  public Expr reset() {
    // Literals are always analyzed, don't call super method as it will clear
    // the analysis flag. Clear any implicit type.
    type_ = explicitType_;
    return this;
  }

  /**
   * Explicitly cast this literal to 'targetType'. The targetType must be a
   * float point type.
   */
  protected void explicitlyCastToFloat(Type targetType) {
    Preconditions.checkState(targetType.isFloatingPointType());
    try {
      Expr expr = uncheckedCastTo(targetType);
      Preconditions.checkState(expr == this);
    } catch (SqlCastException e) {
      // Should never occur
      throw new IllegalStateException(e);
    }
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
      throw new SqlCastException(value, targetType);
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
      throw new SqlCastException(value, targetType);
    }

    // Truncate (round) extra digits if necessary.
    if (value.scale() > decimalType.decimalScale()) {
      return value.setScale(decimalType.decimalScale(), BigDecimal.ROUND_HALF_UP);
    }

    // Existing value fits, use it.
    return value;
  }

  /**
   * Perform an implicit cast. An implicit cast is added by the analyzer
   * and is assumed to be valid. Does not change the explicit type to
   * avoid instability when repeating type propagation.
   * @throws SqlCastException
   */
  @Override
  protected Expr uncheckedCastTo(Type targetType) throws SqlCastException {
    Preconditions.checkState(targetType.isNumericType());
    try {
      BigDecimal converted = convertValue(value_, targetType);
      if (converted == value_) {
        // Use existing value, cast in place.
        type_ = targetType;
        return this;
      } else {
        // Value changed, create a new literal.
        return new NumericLiteral(converted, targetType);
      }
    } catch (SqlCastException e) {
      return new CastExpr(targetType, this);
    }
  }

  @Override
  public void swapSign() {
    // Swapping the sign may change the type:
    // 128 is a SMALLINT, -128 is a TINYINT
    // Also -<max bigint> starts as DECIMAL(19.0), but
    // will flip to BIGINT in this call.
    value_ = value_.negate();
    try {
      type_ = inferType(value_);
      explicitType_ = type_;
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
        return !fitsInTinyInt(value);
      case SMALLINT:
        return !fitsInSmallInt(value);
      case INT:
        return !fitsInInt(value);
      case BIGINT:
        return !fitsInBigInt(value);
      case FLOAT:
        return !fitsInFloat(value);
      case DOUBLE:
        return !fitsInDouble(value);
      case DECIMAL:
        return !isDecimal(value);
      default:
        throw new IllegalArgumentException("Overflow check on " + type.toSql() +
            " isn't supported.");
    }
  }
}
