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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.impala.common.serialize.JsonTreeFormatter.TreeBuilder;
import org.junit.Test;

/**
 * Test the basics of the JSON tree formatter.
 */
public class TreeFormatterTest {
  private abstract static class BuilderTest {
    private final StringWriter strWriter_ = new StringWriter();
    protected final TreeBuilder builder_ = new TreeBuilder(new PrintWriter(strWriter_));

    protected BuilderTest() {
      build();
    }

    protected abstract void build();

    protected void verify(String expected) {
      builder_.close();
      assertEquals(expected, strWriter_.toString());
    }
  }

  /**
   * Test the internal JSON builder which emits the special, compact
   * variant of JSON used here.
   */
  @Test
  public void testBuilder() {
    // Trivial case
    new BuilderTest() {
      @Override
      public void build() {}
    }.verify("");

    // Trivial root
    new BuilderTest() {
      @Override
      public void build() {
        assertEquals(1, builder_.root());
      }
    }.verify("{\n}\n");

    // Fields
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        builder_.unquotedField(level, "f1", null);
        builder_.unquotedField(level, "f2", 10);
        builder_.quotedField(level, "f3", null);
        builder_.quotedField(level, "f4", "abc");
      }
    }.verify("{\n  f1: null,\n  f2: 10,\n  f3: null,\n  f4: \"abc\"\n}\n");

    // Text with special characters
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.objectElement(0);
        builder_.quotedField(level, "f", "abc \"foo\nbar\\\"");
      }
    }.verify("{\n  f: \"abc \\\"foo\nbar\\\\\\\"\"\n}\n");

    // Block text
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        builder_.textField(level, "f", "abc \"foo\nbar\\\"");
      }
    }.verify("{\n  f:\n\"abc \\\"foo\nbar\\\\\\\"\"\n}\n");

    // Quoted fields
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        builder_.unquotedField(level, "f_1", null);
        builder_.unquotedField(level, "f 2", 10);
        builder_.quotedField(level, "f-3", null);
        builder_.quotedField(level, "f.4", "abc");
      }
    }.verify(
        "{\n  f_1: null,\n  \"f 2\": 10,\n  \"f-3\": null,\n  \"f.4\": \"abc\"\n}\n");

    // Empty nested object at end
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        builder_.unquotedField(level, "f1", 10);
        assertEquals(2, builder_.objectField(level, "obj"));
      }
    }.verify("{\n  f1: 10,\n  obj: {\n  }\n}\n");

    // Nested object at end
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        int level2 = builder_.objectField(level, "obj");
        builder_.unquotedField(level2, "g", 20);
      }
    }.verify("{\n  obj: {\n    g: 20\n  }\n}\n");

    // Nested object in middle
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        int level2 = builder_.objectField(level, "obj");
        builder_.unquotedField(level2, "g", 20);
        builder_.unquotedField(level, "h", 30);
      }
    }.verify("{\n  obj: {\n    g: 20\n  },\n  h: 30\n}\n");

    // Two roots
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        builder_.unquotedField(level, "f", 10);
        assertEquals(1, builder_.root());
        builder_.unquotedField(level, "f", 20);
      }
    }.verify("{\n  f: 10\n},\n{\n  f: 20\n}\n");

    // Two nested objects
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        int level2 = builder_.objectField(level, "obj1");
        builder_.unquotedField(level2, "g", 10);
        assertEquals(level2, builder_.objectField(level, "obj2"));
        builder_.unquotedField(level2, "g", 20);
      }
    }.verify("{\n  obj1: {\n    g: 10\n  },\n  obj2: {\n    g: 20\n  }\n}\n");

    // Empty Array
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        assertEquals(2, builder_.arrayField(level, "a"));
      }
    }.verify("{\n  a: [\n  ]\n}\n");

    // Array
    new BuilderTest() {
      @Override
      public void build() {
        int level = builder_.root();
        int level2 = builder_.arrayField(level, "a");
        builder_.unquotedElement(level2, 10);
        builder_.unquotedElement(level2, null);
        builder_.quotedElement(level2, "foo");
        builder_.quotedElement(level2, null);
      }
    }.verify("{\n  a: [\n    10,\n    null,\n    \"foo\",\n    null\n  ]\n}\n");
  }

  public static class DummyObject implements JsonSerializable {
    int value_;

    public DummyObject(int value) { value_ = value; }

    @Override
    public void serialize(ObjectSerializer os) {
      os.field("d", value_);
    }
  }

  private abstract static class JTFTest {
    JsonTreeFormatter fmt_;

    protected JTFTest() {
      this(ToJsonOptions.full());
    }

    protected JTFTest(ToJsonOptions options) {
      fmt_ = new JsonTreeFormatter(options);
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
    new JTFTest() {
      @Override
      protected void build() {}
    }.verify("{\n}\n");

    // Boolean field
    new JTFTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("bool", true);
      }
    }.verify("{\n  bool: true\n}\n");

    // Numeric field
    new JTFTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("long", 5);
      }
    }.verify("{\n  long: 5\n}\n");

    // Quoted string field
    new JTFTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("str", "foo");
      }
    }.verify("{\n  str: \"foo\"\n}\n");

    // Null field
    new JTFTest() {
      @Override
      protected void build() {
        ObjectSerializer os = fmt_.root();
        os.field("null", null);
      }
    }.verify("{\n  null: null\n}\n");

    // Duplicate objects, no dedup
    new JTFTest(ToJsonOptions.fullCompact()) {
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
    new JTFTest(ToJsonOptions.fullCompact().elide(false)) {
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
    new JTFTest(ToJsonOptions.fullCompact()) {
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
    new JTFTest(ToJsonOptions.fullCompact().elide(false)) {
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
