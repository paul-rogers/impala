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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InvalidValueException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.junit.Test;


// Test creation of LiteralExprs from Strings, e.g., for partitioning keys.
public class LiteralExprTest {
  @Test
  public void testValid() {
    // TODO: This is not much of a test, values are not
    // verified.
    testLiteralExprPositive("false", Type.BOOLEAN);
    testLiteralExprPositive("1", Type.TINYINT);
    testLiteralExprPositive("1", Type.SMALLINT);
    testLiteralExprPositive("1", Type.INT);
    testLiteralExprPositive("1", Type.BIGINT);
    testLiteralExprPositive("1.0", Type.FLOAT);
    testLiteralExprPositive("1.0", Type.DOUBLE);
    testLiteralExprPositive("ABC", Type.STRING);
    testLiteralExprPositive("1.1", ScalarType.createDecimalType(2, 1));
  }

  @Test
  public void testInvalidValues() {
    // Invalid casts
    testInvalidValue("ABC", Type.BOOLEAN);
    testInvalidValue("ABC", Type.TINYINT);
    testInvalidValue("ABC", Type.SMALLINT);
    testInvalidValue("ABC", Type.INT);
    testInvalidValue("ABC", Type.BIGINT);
    testInvalidValue("ABC", Type.FLOAT);
    testInvalidValue("ABC", Type.DOUBLE);
    testUnsupported("ABC", Type.TIMESTAMP);
    testInvalidValue("ABC", ScalarType.createDecimalType());

    // NAN and INF are valid Java double values,
    // but are not supported by BigDecimal
    testInvalidValue("NaN", Type.DOUBLE);
    testInvalidValue("INF", Type.DOUBLE);

    // Invalid decimal values
    testInvalidValue("123.1", ScalarType.createDecimalType(2, 1));
    testInvalidValue("123.1", ScalarType.createDecimalType(4, 2));
  }

  @Test
  public void testUnimplementedTypes() {
    // INVALID_TYPE should always fail
    testUnsupported("ABC", Type.INVALID);
    // Date types not implemented
    testUnsupported("2010-01-01", Type.DATE);
    testUnsupported("2010-01-01", Type.DATETIME);
    testUnsupported("2010-01-01", Type.TIMESTAMP);
    testUnsupported("ABC", Type.BINARY);
  }

  private void testLiteralExprPositive(String value, Type type) {
    try {
      LiteralExpr expr = LiteralExpr.create(value, type);
      assertNotNull("Failed to create LiteralExpr", expr);
    } catch (Exception e) {
      fail("Failed to create LiteralExpr of type: " + type.toString() + " from: " + value
          + " due to " + e.getMessage());
    }
  }

  private void testUnsupported(String value, Type type) {
    try {
      LiteralExpr.create(value, type);
      fail("\nUnexpectedly succeeded to create LiteralExpr of type: "
          + type.toString() + " from: " + value);
    } catch (UnsupportedFeatureException e) {
      // Expected
    } catch (AnalysisException e) {
      fail();
    }
  }

  private void testInvalidValue(String value, Type type) {
    try {
      LiteralExpr.create(value, type);
      fail("\nUnexpectedly succeeded to create LiteralExpr of type: "
          + type.toString() + " from: " + value);
    } catch (InvalidValueException e) {
      // Expected
    } catch (AnalysisException e) {
      fail();
    }
  }
}
