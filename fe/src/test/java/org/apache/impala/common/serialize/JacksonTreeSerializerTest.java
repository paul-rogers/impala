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

package org.apache.impala.common.serialize;

import static org.junit.Assert.assertEquals;

import org.apache.impala.common.serialize.JacksonTreeSerializer.JacksonTreeStringSerializer;
import org.junit.Test;

/**
 * Test the basics of the Jackson-based tree serializer.
 */
public class JacksonTreeSerializerTest {
  public static class DummyObject implements JsonSerializable {
    int value_;

    public DummyObject(int value) { value_ = value; }

    @Override
    public void serialize(ObjectSerializer os) {
      os.field("d", value_);
    }
  }

  private abstract static class JTSTest {
    JacksonTreeStringSerializer fmt_;

    protected JTSTest() {
      this(ToJsonOptions.full());
    }

    protected JTSTest(ToJsonOptions options) {
      fmt_ = JacksonTreeStringSerializer.create(options);
      build();
    }

    protected abstract void build();

    protected void verify(String expected) {
      assertEquals(expected, fmt_.toString());
    }
  }

  /**
   * Test the JSON tree formatter implementation of the Tree
   * serializer. Verifies that indentation, elision and other
   * features work as expected.
   */
  @Test
  public void testSerialize() {
    // Trivial
    new JTSTest() {
      @Override
      protected void build() {}
    }.verify("{ }\n");

    // Boolean field
    new JTSTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("bool", true);
      }
    }.verify("{\n  bool: true\n}\n");

    // Numeric field
    new JTSTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("long", 5);
      }
    }.verify("{\n  long: 5\n}\n");

    // Quoted string field
    new JTSTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("str", "foo");
      }
    }.verify("{\n  str: \"foo\"\n}\n");

    // Null field
    new JTSTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("null", null);
      }
    }.verify("{\n  null: null\n}\n");

    // Duplicate objects, no dedup
    new JTSTest(ToJsonOptions.fullCompact()) {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        DummyObject obj = new DummyObject(10);
        os.object("first", obj);
        os.object("second", new DummyObject(20));
        os.object("third", null);
        os.object("fourth", obj);
      }
    }.verify(
        "{\n  first: {\n    object_id: 1,\n    d: 10\n  },\n  second: {\n" +
        "    object_id: 2,\n    d: 20\n  },\n  fourth: \"<1>\"\n}\n");

    // Duplicate objects with dedup
    new JTSTest(ToJsonOptions.fullCompact().elide(false)) {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        DummyObject obj = new DummyObject(10);
        os.object("first", obj);
        os.object("second", new DummyObject(20));
        os.object("third", null);
        os.object("fourth", obj);
      }
    }.verify(
        "{\n  first: {\n    object_id: 1,\n    d: 10\n  },\n  second: {\n" +
        "    object_id: 2,\n    d: 20\n  },\n  third: null,\n" +
        "  fourth: \"<1>\"\n}\n");

    // Elision
    new JTSTest(ToJsonOptions.fullCompact()) {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("f1", true);
        os.field("f2", false);
        os.elidable("f3", true);
        os.elidable("f4", false);
        os.field("f5", "foo");
        os.field("f6", null);
      }
    }.verify(
        "{\n  f1: true,\n  f2: false,\n  f3: true,\n  f5: \"foo\"\n}\n");

    // No elision
    new JTSTest(ToJsonOptions.fullCompact().elide(false)) {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("f1", true);
        os.field("f2", false);
        os.elidable("f3", true);
        os.elidable("f4", false);
        os.field("f5", "foo");
        os.field("f6", null);
      }
    }.verify(
        "{\n  f1: true,\n  f2: false,\n  f3: true,\n  f4: false,\n" +
        "  f5: \"foo\",\n  f6: null\n}\n");
  }
}
